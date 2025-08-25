import os,sys,requests
from zipfile import ZipFile

def download_zip_file(url, output_dir):
    response = requests.get(url, stream=True)
    os.makedirs(output_dir, exist_ok=True)
    
    if response.status_code == 200:
        filename = os.path.join(output_dir, "downloaded.zip")
        with open(filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        print(f"Downloaded zip file: {filename}")
        return filename
    else:
        raise Exception(f"Failed to download file. Status code: {response.status_code}")

def extract_zip_file(zip_filename, output_dir):
    with ZipFile(zip_filename, "r") as zip_file:
        zip_file.extractall(output_dir)
    print(f"Extracted files written to: {output_dir}")
    print("Removing the zip file")
    os.remove(zip_filename)

def fix_json_dict(output_dir):
    import json 
    file_path = os.path.join(output_dir, "dict_artists.json")
    
    with open(file_path, "r") as f:
        data = json.load(f)
    
    with open(os.path.join(output_dir, "fixed_da.json"), "w", encoding="utf-8") as f_out:
        for key, value in data.items():
            record = {"id": key, "related ids": value}
            json.dump(record, f_out, ensure_ascii=False)
            f_out.write("\n")
    
    print(f"File {file_path} has been fixed and written to {output_dir} as fixed_da.json")
    print("Removing the original file")
    os.remove(file_path)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Extraction path is required")
        print("Example Usage:")
        print("python3 execute.py /home/ardent-sharma/Data/Extraction")
    else:
        try:
            print("Starting Extraction Engine...")
            EXTRACT_PATH = sys.argv[1]
            KAGGLE_URL = "https://storage.googleapis.com/kaggle-data-sets/1993933/3294812/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20250801%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20250801T022732Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=b296ed9806c1ed9d867354d4477daff143ab18d400e47fcefd93b61d29f65054b1b58e4c329f43ede393503339021f1981db1db2326936287d762566d09fdbe4f40024b7f35d5dcdf47ba5baaeaeb8edf3584426f92408fce307b982a5004141ca692d0188001c419a4c3fd92a0c44255e854099dd43d203d4a6ca49040070269a26dcd305733aab957d2a2edf3aa7fbb5e66b3c310d5cbca9109019c8bf9099ba0355532c9ad24347f7e98bed3e6d7acffe1259264005cd2975fa46b01aaa049286c24f06117222d36085a4c6979a9b286a859c615e5fca6f5b66b142330052c03bd5f6b8d5d782dce9284c32150ce4c5a0cced0871ffd5c9f7162161738c80"
            zip_filename = download_zip_file(KAGGLE_URL, EXTRACT_PATH)
            extract_zip_file(zip_filename, EXTRACT_PATH)
            fix_json_dict(EXTRACT_PATH)
            print("Extraction Successfully Completed!!!")
        except Exception as e:
            print(f"Error: {e}")
