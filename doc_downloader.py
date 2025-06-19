import os
import re
import argparse
import requests
import pandas as pd
from urllib.parse import urlparse

# Create a session with a default User-Agent to help avoid 403 errors
session = requests.Session()
def extract_urls(text):
    """Extract all PDF URLs from a text string."""
    if not isinstance(text, str):
        return []
    return re.findall(r'https?://[^\s,;]+\.pdf', text)


def sanitize_filename(name):
    """Sanitize a string to be safe for filenames."""
    safe = re.sub(r'[^A-Za-z0-9_\-]', '_', str(name))
    return safe.strip('_') or 'unknown'


def download_pdf(url, output_path, timeout=10, max_retries=1):
    """Download a PDF from a URL to the given output path, retrying once on 403 with a browser-style UA."""
    headers = session.headers.copy()
    for attempt in range(max_retries + 1):
        try:
            # Use session to leverage persistent headers/cookies
            response = session.get(url, stream=True, timeout=timeout, headers=headers)
            if response.status_code == 403 and attempt == 0:
                # Retry with explicit browser UA
                print(f"403 Forbidden for {url}, retrying with browser User-Agent...")
                headers['User-Agent'] = (
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/115.0 Safari/537.36'
                )
                continue
            response.raise_for_status()
            content_type = response.headers.get('Content-Type', '')
            if 'application/pdf' not in content_type:
                print(f"Warning: {url} returned content-type {content_type}")
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
            return True
        except requests.HTTPError as http_err:
            print(f"HTTP error for {url}: {http_err}")
            return False
        except Exception as e:
            print(f"Error downloading {url}: {e}")
            return False
    return False


def main():
    parser = argparse.ArgumentParser(
        description='Download PDFs from URLs listed in an Excel or CSV file into country/year subfolders.')
    parser.add_argument('input_file', help='Path to input CSV or Excel file')
    parser.add_argument(
        '--output-dir', default='downloads', help='Base directory to save downloaded PDFs')
    parser.add_argument(
        '--user-agent', help='Custom User-Agent header to use when downloading',
        default='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                '(KHTML, like Gecko) Chrome/115.0 Safari/537.36'
    )
    args = parser.parse_args()

    # Set session User-Agent
    session.headers.update({'User-Agent': args.user_agent})

    # Create base output directory
    os.makedirs(args.output_dir, exist_ok=True)

    # Load data
    ext = os.path.splitext(args.input_file)[1].lower()
    if ext in ('.xls', '.xlsx'):
        df = pd.read_excel(args.input_file)
    elif ext == '.csv':
        df = pd.read_csv(args.input_file)
    else:
        print('Unsupported file format. Provide a .csv, .xls, or .xlsx file.')
        return

    report = []  # Track documents with failed downloads

    for idx, row in df.iterrows():
        title = sanitize_filename(row.get('English name', f'doc_{idx}'))
        country = sanitize_filename(row.get('Country', 'unknown'))

        # Build output subdirectory: base/country/year
        subdir = os.path.join(args.output_dir, country)
        os.makedirs(subdir, exist_ok=True)

        urls = extract_urls(row.get('Public access URL', ''))
        failed = []

        for url in urls:
            parsed = urlparse(url)
            base = os.path.basename(parsed.path)
            if not base.lower().endswith('.pdf'):
                base += '.pdf'
            filename = f"{title}_{base}"
            output_path = os.path.join(subdir, filename)

            if not download_pdf(url, output_path):
                failed.append(url)

        if failed:
            report.append({'title': title, 'country': country, 'failed_urls': failed})

    # Summary
    if report:
        print("\nDownloads completed with some errors:")
        for item in report:
            print(f"- {item['title']} ({item['country']}): "
                  f"Failed {len(item['failed_urls'])} URL(s)")
            for u in item['failed_urls']:
                print(f"    • {u}")
    else:
        print("All PDFs downloaded successfully.")

if __name__ == '__main__':
    main()