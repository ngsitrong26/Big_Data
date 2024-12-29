import json
import subprocess
import os

def store_data_in_hdfs(url, text, metadata):
    """
    Lưu nội dung của mỗi website vào một file JSON riêng biệt và upload lên HDFS từ container NameNode.

    Args:
        url (str): URL của website.
        text (str): Nội dung của website.
        metadata (dict): Metadata bao gồm title, description, keywords.
    """
    output_dir = 'F:/ITTN_Project/Big_Data/Big-Data-Project/Lambda/Stream_data/'
    hdfs_dir = '/batch-layer/'
    container_name = '2bc42411b54f'

    # Đảm bảo thư mục output tồn tại
    os.makedirs(output_dir, exist_ok=True)

    # Tạo tên file từ URL (thay thế ký tự không hợp lệ)
    filename = url.replace("http://", "").replace("https://", "").replace("/", "_").replace(":", "_").replace(".", "_")
    filename = f"{filename}.json"

    # Đường dẫn đầy đủ của file JSON cục bộ
    local_file_path = os.path.join(output_dir, filename)

    # Chuẩn bị dữ liệu
    website_data = {
        'url': url,
        'text': text,
        'metadata': {
            'title': metadata.get('title', ''),
            'description': metadata.get('description', ''),
            'keywords': metadata.get('keywords', [])
        }
    }

    # Lưu vào file JSON cục bộ
    try:
        with open(local_file_path, 'w', encoding='utf-8') as json_file:
            json.dump(website_data, json_file, ensure_ascii=False, indent=4)
        print(f"Saved website data to: {local_file_path}")
    except Exception as e:
        print(f"Error saving website data for {url}: {e}")
        return

    # Copy file từ máy host vào container
    container_file_path = f"/tmp/{filename}"
    try:
        docker_cp_command = [
            "docker", "cp", local_file_path, f"{container_name}:{container_file_path}"
        ]
        result = subprocess.run(docker_cp_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        if result.returncode == 0:
            print(f"Successfully copied {local_file_path} to container at {container_file_path}")
        else:
            print(f"Failed to copy file to container. Error: {result.stderr}")
            return
    except Exception as e:
        print(f"Error copying file to container: {e}")
        return

    # Upload file từ container lên HDFS
    try:
        hdfs_file_path = os.path.join(hdfs_dir, filename)
        docker_exec_command = [
            "docker", "exec", container_name, "hdfs", "dfs", "-put", "-f", container_file_path, hdfs_file_path
        ]
        result = subprocess.run(docker_exec_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        if result.returncode == 0:
            print(f"Successfully uploaded {container_file_path} to HDFS at {hdfs_file_path}")
        else:
            print(f"Failed to upload file to HDFS. Error: {result.stderr}")
    except Exception as e:
        print(f"Error uploading file to HDFS: {e}")
