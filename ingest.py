import json
import requests
import docx
import pypdf
from bs4 import BeautifulSoup
from pathlib import Path
from typing import List, Dict, Union

# Configuration
DOCUMENTS_DIR = Path("documents")
URLS_FILE = Path("urls.txt")
OUTPUT_FILE = Path("extracted_data.json")

# extraction functions

def extract_from_pdf(file_path: Path) -> str:
    print(f"processing PDF: {file_path.name}")
    try:
        reader = pypdf.PdfReader(file_path)
        return "\n".join(page.extract_text() for page in reader.pages if page.extract_text())
    except Exception as e:
        print(f"error reading PDF {file_path.name}: {e}")
        return ""

def extract_from_docx(file_path: Path) -> str:
    print(f"processing DOCX: {file_path.name}")
    try:
        doc = docx.Document(file_path)
        return "\n".join(para.text for para in doc.paragraphs if para.text)
    except Exception as e:
        print(f"error reading DOCX {file_path.name}: {e}")
        return ""

def extract_from_txt(file_path: Path) -> str:
    print(f"processing TXT: {file_path.name}")
    try:
        return file_path.read_text(encoding="utf-8")
    except Exception as e:
        print(f"error reading TXT {file_path.name}: {e}")
        return ""

def extract_from_url(url: str) -> str:
    print(f"processing URL: {url}")
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # strip out script and style elements
        for script_or_style in soup(["script", "style"]):
            script_or_style.decompose()
        
        return soup.body.get_text(separator='\n', strip=True)
    except requests.RequestException as e:
        print(f"error fetching URL {url}: {e}")
        return ""

# Main logic

def main():
    all_data: List[Dict[str, str]] = []

    # process local files
    if DOCUMENTS_DIR.is_dir():
        for file_path in DOCUMENTS_DIR.iterdir():
            content = ""
            if file_path.suffix == '.pdf':
                content = extract_from_pdf(file_path)
            elif file_path.suffix == '.docx':
                content = extract_from_docx(file_path)
            elif file_path.suffix == '.txt':
                content = extract_from_txt(file_path)
            
            if content:
                all_data.append({"source": file_path.name, "content": content})
    else:
        print(f"Documents directory '{DOCUMENTS_DIR}' not found. Skipping local file processing.")

    # process URLs from file
    if URLS_FILE.is_file():
        with open(URLS_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                url = line.strip()
                if url and not url.startswith('#'):
                    content = extract_from_url(url)
                    if content:
                        all_data.append({"source": url, "content": content})
    else:
        print(f"URL file '{URLS_FILE}' not found. Skipping web processing.")

    # writing standardized output
    if not all_data:
        print("\nNo data was extracted. No output file created")
        return

    try:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, indent=2, ensure_ascii=False)
        print(f"\nIngestion complete. {len(all_data)} documents processed and saved to '{OUTPUT_FILE}'.")
    except IOError as e:
        print(f"\nCould not write to output file '{OUTPUT_FILE}': {e}")


if __name__ == "__main__":
    main()