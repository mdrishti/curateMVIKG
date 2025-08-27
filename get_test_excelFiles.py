import os
import tarfile


def extract_xls_from_targz(directory: str, output_dir: str = "extracted_xls") -> None:
    os.makedirs(output_dir, exist_ok=True)

    for filename in os.listdir(directory):
        if filename.endswith(".tar.gz") or filename.endswith(".tgz"):
            file_path = os.path.join(directory, filename)
            try:
                with tarfile.open(file_path, "r:gz") as tar:
                    for member in tar.getmembers():
                        if member.name.endswith(".xls") or member.name.endswith(".xlsx"):
                            member.name = os.path.basename(member.name)
                            tar.extract(member, output_dir)
                            print(f"Extracted: {member.name} from {filename}")
            except Exception as e:
                print(f"Error processing {filename}: {e}")


if __name__ == "__main__":
    target_directory = input("Enter directory path: ").strip()
    extract_xls_from_targz(target_directory)
  
