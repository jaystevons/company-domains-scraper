#!/usr/bin/env python3
"""
Company Domain Scraper for Yahoo Finance
Extracts company domains from Yahoo Finance profile pages
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
    """Get company website from Yahoo Finance profile page"""
    
    url = f"https://finance.yahoo.com/quote/{ticker}/profile/"
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Look for website links
        website = 'N/A'
        
        # Find external links that look like company websites
        links = soup.find_all('a', href=True)
        for link in links:
            href = link.get('href', '')
            if (href and 
                href.startswith('http') and 
                len(href) > 10 and
                not any(skip in href.lower() for skip in ['yahoo', 'finance.yahoo', 'javascript:', 'mailto:', '#', 'twitter', 'facebook', 'linkedin'])):
                website = href
                break
        
        return website
        
    except Exception as e:
        print(f"Error with {ticker}: {str(e)}")
        return 'N/A'

def main():
    """Main function"""
    
    # File names
    ticker_file = "tickers.csv"
    output_file = "company_domains.csv"
    
    # Check if ticker file exists
    if not os.path.exists(ticker_file):
        print(f"❌ Error: {ticker_file} not found!")
        print("Please create a tickers.csv file with one ticker per line")
        print("Example:")
        print("AAPL")
        print("GOOGL")
        print("MSFT")
        return
    
    # Read ticker symbols
    print("📖 Reading ticker symbols...")
    tickers = []
    
    try:
        if ticker_file.endswith('.csv'):
            df = pd.read_csv(ticker_file)
            tickers = df.iloc[:, 0].tolist()
        else:
            with open(ticker_file, 'r') as f:
                tickers = [line.strip().upper() for line in f if line.strip()]
    except Exception as e:
        print(f"❌ Error reading {ticker_file}: {e}")
        return
    
    if not tickers:
        print("❌ No ticker symbols found!")
        return
        
    print(f"✅ Found {len(tickers)} ticker symbols")
    
    # Process tickers
    results = []
    failed_count = 0
    
    print(f"\n🚀 Starting to scrape {len(tickers)} companies...")
    print("This will take approximately", len(tickers) * 2, "seconds")
    
    for i, ticker in enumerate(tickers, 1):
        print(f"[{i}/{len(tickers)}] Processing {ticker}...", end=' ')
        
        website_url = get_company_website(ticker)
        domain = extract_domain_from_url(website_url)
        
        if domain == 'N/A':
            failed_count += 1
            print("❌")
        else:
            print(f"✅ {domain}")
        
        results.append({
            'Ticker': ticker,
            'Website_URL': website_url,
            'Domain': domain
        })
        
        # Respectful delay
        time.sleep(2)
    
    # Save results
    print(f"\n💾 Saving results...")
    
    try:
        df_results = pd.DataFrame(results)
        df_results.to_csv(output_file, index=False)
        
        # Create domain-only file
        domain_only_file = "domains_only.txt"
        unique_domains = sorted(set([r['Domain'] for r in results if r['Domain'] != 'N/A']))
        
        with open(domain_only_file, 'w') as f:
            for domain in unique_domains:
                f.write(domain + '\n')
        
        # Print summary
        print(f"\n🎉 Scraping Complete!")
        print(f"📊 Results:")
        print(f"   • Total companies processed: {len(tickers)}")
        print(f"   • Domains found: {len(unique_domains)}")
        print(f"   • Failed to find: {failed_count}")
        print(f"\n📁 Files created:")
        print(f"   • {output_file} (detailed results)")
        print(f"   • {domain_only_file} (clean domain list)")
        
    except Exception as e:
        print(f"❌ Error saving results: {e}")

if __name__ == "__main__":
    main()
