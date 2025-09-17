#!/usr/bin/env python3
"""
Company Website Scraper for stockanalysis.com
Extracts company websites from the Contact Details section
Respects rate limits with conservative delays
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from urllib.parse import urlparse
import sys
import os
import random

def extract_domain_from_url(url):
    """Extract clean domain name from URL"""
    if not url or url.strip() == '' or url == 'N/A':
        return 'N/A'
    
    try:
        # Clean up the URL
        url = str(url).strip()
        
        # Add protocol if missing
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        # Parse the URL
        parsed = urlparse(url)
        domain = parsed.netloc
        
        # Remove 'www.' prefix if present
        if domain.startswith('www.'):
            domain = domain[4:]
        
        return domain if domain else 'N/A'
    except:
        return 'N/A'

def get_company_website(ticker):
    """Get company website from stockanalysis.com"""
    
    # Convert ticker to lowercase for the URL
    ticker_lower = ticker.lower()
    url = f"https://stockanalysis.com/stocks/{ticker_lower}/company/"
    
    try:
        # Conservative headers to appear like a regular browser
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
        }
        
        response = requests.get(url, headers=headers, timeout=15)
        
        # Check for rate limiting or blocking
        if response.status_code == 429:
            print(f"Rate limited on {ticker}")
            return 'RATE_LIMITED', url
        elif response.status_code == 404:
            print(f"Company not found: {ticker}")
            return 'NOT_FOUND', url
        elif response.status_code != 200:
            print(f"HTTP {response.status_code} for {ticker}")
            return 'ERROR', url
        
        # Parse the HTML content
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Look for the Contact Details section and find Website
        # Based on the structure: there's a table with "Website" label
        website_url = 'N/A'
        
        # Method 1: Look for table cells containing "Website"
        cells = soup.find_all(['td', 'th', 'div', 'span'])
        for i, cell in enumerate(cells):
            if cell.get_text().strip().lower() == 'website':
                # Look for the next cell or nearby elements for the actual URL
                try:
                    # Check next sibling
                    next_cell = cells[i + 1] if i + 1 < len(cells) else None
                    if next_cell:
                        # Look for links in the next cell
                        link = next_cell.find('a', href=True)
                        if link:
                            website_url = link.get('href')
                            break
                        # Or just text content that looks like a URL
                        text = next_cell.get_text().strip()
                        if text and ('.' in text) and (text.startswith(('http', 'www')) or '.com' in text):
                            website_url = text
                            break
                except:
                    continue
        
        # Method 2: Look for any links in Contact Details area that look like company websites
        if website_url == 'N/A':
            # Find the contact details section
            contact_sections = soup.find_all(text=lambda x: x and 'contact' in x.lower())
            for section in contact_sections:
                parent = section.parent
                if parent:
                    # Look for links within this section
                    for link in parent.find_all('a', href=True):
                        href = link.get('href', '').strip()
                        if href and not any(skip in href.lower() for skip in [
                            'stockanalysis.com', 'mailto:', 'tel:', 'javascript:',
                            'facebook.', 'twitter.', 'linkedin.', 'youtube.'
                        ]):
                            if href.startswith(('http', 'www')) or '.com' in href:
                                website_url = href
                                break
                if website_url != 'N/A':
                    break
        
        # Method 3: Look for any external links that could be company websites
        if website_url == 'N/A':
            all_links = soup.find_all('a', href=True)
            for link in all_links:
                href = link.get('href', '').strip()
                if (href and 
                    not any(skip in href.lower() for skip in [
                        'stockanalysis.com', 'mailto:', 'tel:', 'javascript:',
                        'facebook.', 'twitter.', 'linkedin.', 'youtube.',
                        'sec.gov', 'edgar'
                    ]) and
                    (href.startswith(('http', 'www')) or '.com' in href)):
                    website_url = href
                    break
        
        return website_url, url
        
    except requests.exceptions.RequestException as e:
        print(f"Request error for {ticker}: {e}")
        return 'ERROR', url
    except Exception as e:
        print(f"Parse error for {ticker}: {e}")
        return 'ERROR', url

def save_progress(results, output_file):
    """Save current progress to files"""
    try:
        # Save main results
        df_results = pd.DataFrame(results)
        df_results.to_csv(output_file, index=False)
        
        # Save domain-only file
        domain_only_file = "stockanalysis_domains.txt"
        successful_results = [r for r in results if r['Domain'] != 'N/A' and r['Website_URL'] not in ['ERROR', 'RATE_LIMITED', 'NOT_FOUND']]
        unique_domains = sorted(set([r['Domain'] for r in successful_results]))
        
        with open(domain_only_file, 'w') as f:
            for domain in unique_domains:
                f.write(domain + '\n')
        
        print(f"PROGRESS SAVED: {len(results)} results, {len(unique_domains)} unique domains")
        return len(unique_domains)
        
    except Exception as e:
        print(f"Save error: {e}")
        return 0

def main():
    """Main scraping function"""
    
    print("StockAnalysis.com Company Website Scraper")
    print("=" * 50)
    print("Conservative rate limiting: 3-5 seconds between requests")
    
    # File setup
    ticker_files = ["tickers.csv", "tickers.txt"]
    ticker_file = None
    
    for file in ticker_files:
        if os.path.exists(file):
            ticker_file = file
            break
    
    if not ticker_file:
        print("No ticker file found! Please create 'tickers.csv' or 'tickers.txt'")
        return
    
    output_file = "stockanalysis_results.csv"
    
    # Read tickers
    print(f"Reading from: {ticker_file}")
    tickers = []
    
    try:
        if ticker_file.endswith('.csv'):
            df = pd.read_csv(ticker_file)
            tickers = df.iloc[:, 0].dropna().astype(str).str.strip().str.upper().tolist()
        else:
            with open(ticker_file, 'r') as f:
                tickers = [line.strip().upper() for line in f if line.strip()]
    except Exception as e:
        print(f"Error reading {ticker_file}: {e}")
        return
    
    # Remove duplicates
    tickers = list(dict.fromkeys(tickers))
    
    # Check for existing results (resume capability)
    existing_results = []
    if os.path.exists(output_file):
        try:
            existing_df = pd.read_csv(output_file)
            existing_results = existing_df.to_dict('records')
            completed_tickers = set(existing_df['Ticker'].tolist())
            tickers = [t for t in tickers if t not in completed_tickers]
            print(f"Resume: {len(existing_results)} completed, {len(tickers)} remaining")
        except:
            print("Starting fresh")
    
    if not tickers:
        print("All tickers already completed!")
        return
    
    print(f"Processing {len(tickers)} tickers")
    estimated_time = len(tickers) * 4  # 4 seconds average per ticker
    print(f"Estimated time: {estimated_time//60} minutes")
    
    # Process tickers
    results = existing_results.copy()
    success_count = len([r for r in existing_results if r['Domain'] != 'N/A'])
    failed_count = 0
    not_found_count = 0
    rate_limited_count = 0
    
    print(f"\nStarting scraping...")
    
    for i, ticker in enumerate(tickers, 1):
        current_total = len(results) + 1
        total_expected = len(existing_results) + len(tickers)
        print(f"[{current_total:4}/{total_expected:4}] {ticker:<8}", end=' ')
        
        # Get website
        website_url, source_url = get_company_website(ticker)
        
        if website_url == 'RATE_LIMITED':
            rate_limited_count += 1
            print("RATE LIMITED - waiting longer...")
            time.sleep(30)  # Wait longer before continuing
            continue
        elif website_url == 'NOT_FOUND':
            not_found_count += 1
            print("Company not found")
        elif website_url == 'ERROR':
            failed_count += 1
            print("Error occurred")
        else:
            domain = extract_domain_from_url(website_url)
            if domain != 'N/A':
                success_count += 1
                print(f"✓ {domain}")
            else:
                failed_count += 1
                print("No website found")
        
        # Add to results
        results.append({
            'Ticker': ticker,
            'Website_URL': website_url,
            'Domain': extract_domain_from_url(website_url),
            'Source_URL': source_url
        })
        
        # Save every 25 entries
        if len(results) % 25 == 0:
            save_progress(results, output_file)
        
        # Progress summary every 50
        if i % 50 == 0:
            print(f"\nProgress: {i}/{len(tickers)} | Success: {success_count} | Failed: {failed_count} | Not Found: {not_found_count}")
        
        # Conservative delay: 3-5 seconds between requests
        delay = random.uniform(3, 5)
        time.sleep(delay)
    
    # Final save
    print(f"\nFinal save...")
    domains_found = save_progress(results, output_file)
    
    # Final summary
    total_processed = len(results)
    success_rate = (success_count / total_processed) * 100 if total_processed > 0 else 0
    
    print(f"\nScraping Complete!")
    print(f"Results:")
    print(f"  Total processed: {total_processed}")
    print(f"  Websites found: {success_count} ({success_rate:.1f}%)")
    print(f"  Failed: {failed_count}")
    print(f"  Not found: {not_found_count}")
    print(f"  Rate limited: {rate_limited_count}")
    print(f"  Unique domains: {domains_found}")
    print(f"\nFiles:")
    print(f"  {output_file} - Complete results")
    print(f"  stockanalysis_domains.txt - Domain list")

if __name__ == "__main__":
    main()
