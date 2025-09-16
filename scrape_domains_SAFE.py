#!/usr/bin/env python3
"""
WORKING Company Domain Scraper - Based on Your Original That Actually Worked
Just added auto-save every 50 entries to the logic that was already working
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from urllib.parse import urlparse
import sys
import os

def extract_domain_from_url(url):
    """Extract just the domain name from a full URL"""
    if not url or url == 'N/A':
        return 'N/A'
    
    try:
        # Add http:// if no protocol is specified
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        # Parse the URL and extract the domain
        parsed = urlparse(url)
        domain = parsed.netloc
        
        # Remove 'www.' if present
        if domain.startswith('www.'):
            domain = domain[4:]
        
        return domain
    except:
        return 'N/A'

def get_company_website(ticker):
    """Get company website from Yahoo Finance profile page - ORIGINAL WORKING VERSION"""
    
    # Create the Yahoo Finance profile URL
    url = f"https://finance.yahoo.com/quote/{ticker}/profile/"
    
    try:
        # Add headers to avoid being blocked - SAME AS ORIGINAL
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Make the request
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # Parse the HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Look for website information - ORIGINAL WORKING LOGIC
        website = 'N/A'
        
        # Method 1: Look for external links in the profile section
        links = soup.find_all('a', href=True)
        for link in links:
            href = link.get('href', '')
            # Skip Yahoo/internal links and look for external company websites
            if href and not any(skip in href.lower() for skip in ['yahoo', 'finance.yahoo', 'javascript:', 'mailto:', '#']):
                if href.startswith('http') and len(href) > 10:
                    # This is likely an external website link
                    website = href
                    break
        
        # Method 2: Look in specific sections (backup method)
        if website == 'N/A':
            # Sometimes the website is in a specific section
            contact_sections = soup.find_all(['div', 'span', 'p'], text=lambda x: x and ('website' in x.lower() or 'web site' in x.lower()))
            for section in contact_sections:
                parent = section.find_parent()
                if parent:
                    link = parent.find('a', href=True)
                    if link and link.get('href', '').startswith('http'):
                        website = link.get('href')
                        break
        
        return website
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {ticker}: {e}")
        return 'N/A'
    except Exception as e:
        print(f"Error parsing {ticker}: {e}")
        return 'N/A'

def scrape_company_domains(ticker_file, output_file):
    """Main function to scrape domains for all tickers - WITH AUTO-SAVE EVERY 50"""
    
    # Read ticker symbols from file
    print("Reading ticker symbols...")
    tickers = []
    
    try:
        # Try reading as CSV first
        df = pd.read_csv(ticker_file)
        # Assume the ticker symbols are in the first column
        tickers = df.iloc[:, 0].tolist()
    except:
        # If CSV fails, try reading as plain text file
        with open(ticker_file, 'r') as f:
            tickers = [line.strip() for line in f if line.strip()]
    
    print(f"Found {len(tickers)} ticker symbols")
    
    # Check for existing results to resume
    existing_results = []
    if os.path.exists(output_file):
        try:
            existing_df = pd.read_csv(output_file)
            existing_results = existing_df.to_dict('records')
            completed_tickers = set(existing_df['Ticker'].tolist())
            print(f"Found existing results: {len(existing_results)} completed")
            # Remove already completed tickers
            tickers = [t for t in tickers if t not in completed_tickers]
            print(f"Remaining to process: {len(tickers)}")
        except:
            print("Could not load existing results, starting fresh")
    
    # Create results list
    results = existing_results.copy()  # Start with existing results
    
    # Process each ticker
    for i, ticker in enumerate(tickers):
        current_position = len(results) + 1
        print(f"Processing {current_position}/{len(existing_results) + len(tickers)}: {ticker}")
        
        # Get the website URL
        website_url = get_company_website(ticker)
        
        # Extract just the domain name
        domain = extract_domain_from_url(website_url)
        
        # Add to results
        results.append({
            'Ticker': ticker,
            'Website_URL': website_url,
            'Domain': domain
        })
        
        # SAVE EVERY 50 ENTRIES - GUARANTEED SAVE
        if len(results) % 50 == 0:
            try:
                df_results = pd.DataFrame(results)
                df_results.to_csv(output_file, index=False)
                
                # Also create domain-only file
                domain_only_file = output_file.replace('.csv', '_domains_only.txt')
                unique_domains = set([result['Domain'] for result in results if result['Domain'] != 'N/A'])
                
                with open(domain_only_file, 'w') as f:
                    for domain in sorted(unique_domains):
                        f.write(domain + '\n')
                
                print(f"💾 PROGRESS SAVED: {len(results)} total results, {len(unique_domains)} unique domains")
                
            except Exception as save_error:
                print(f"❌ Save error: {save_error}")
        
        # Add a small delay to be respectful to Yahoo's servers
        time.sleep(1)
        
        # Print progress every 10 tickers
        if (i + 1) % 10 == 0:
            print(f"Completed {i+1} new tickers...")
    
    # Final save
    print(f"Saving final results to {output_file}...")
    try:
        df_results = pd.DataFrame(results)
        df_results.to_csv(output_file, index=False)
        
        # Also create a simple domain-only file
        domain_only_file = output_file.replace('.csv', '_domains_only.txt')
        unique_domains = set([result['Domain'] for result in results if result['Domain'] != 'N/A'])
        
        with open(domain_only_file, 'w') as f:
            for domain in sorted(unique_domains):
                f.write(domain + '\n')
        
        print(f"\nComplete! Results saved to:")
        print(f"- Full results: {output_file}")
        print(f"- Domain list only: {domain_only_file}")
        print(f"- Found {len(unique_domains)} unique domains out of {len(results)} total companies")
        
    except Exception as e:
        print(f"❌ Final save error: {e}")

# HOW TO USE THIS SCRIPT:
# 1. Save your ticker symbols in a file called 'tickers.txt' (one per line) or 'tickers.csv'
# 2. Run the script
# 3. Results will be saved to 'company_domains.csv'
# 4. Progress is saved every 50 entries automatically

if __name__ == "__main__":
    # Change these file names if needed
    ticker_input_file = "tickers.txt"  # or "tickers.csv"
    output_file = "company_domains.csv"
    
    # Check which ticker file exists
    if os.path.exists("tickers.csv"):
        ticker_input_file = "tickers.csv"
    elif os.path.exists("tickers.txt"):
        ticker_input_file = "tickers.txt"
    else:
        print("❌ No ticker file found! Please create 'tickers.csv' or 'tickers.txt'")
        sys.exit(1)
    
    print(f"🚀 Using ticker file: {ticker_input_file}")
    print(f"💾 Will save progress every 50 entries to: {output_file}")
    print(f"🔄 Can resume if interrupted")
    
    scrape_company_domains(ticker_input_file, output_file)
