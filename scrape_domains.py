#!/usr/bin/env python3
"""
Company Domain Scraper for Yahoo Finance
Extracts company domains from Yahoo Finance profile pages
Updated version that handles Yahoo's anti-bot measures
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from urllib.parse import urlparse
import sys
import os
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def create_session():
    """Create a requests session with retry strategy and proper headers"""
    session = requests.Session()
    
    # Retry strategy
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def extract_domain_from_url(url):
    """Extract just the domain name from a full URL"""
    if not url or url == 'N/A':
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

def get_random_user_agent():
    """Get a random user agent to avoid detection"""
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    ]
    return random.choice(user_agents)

def get_company_website(ticker, session):
    """Get company website from Yahoo Finance profile page"""
    
    # Try multiple URL formats
    urls_to_try = [
        f"https://finance.yahoo.com/quote/{ticker}/profile/",
        f"https://finance.yahoo.com/quote/{ticker}/profile",
        f"https://finance.yahoo.com/quote/{ticker}/"
    ]
    
    for url in urls_to_try:
        try:
            # Random headers to avoid detection
            headers = {
                'User-Agent': get_random_user_agent(),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Cache-Control': 'max-age=0',
            }
            
            # Make the request
            response = session.get(url, headers=headers, timeout=15)
            
            if response.status_code == 200:
                return parse_website_from_content(response.text, ticker)
            elif response.status_code == 429:  # Rate limited
                print(f"Rate limited on {ticker}, waiting longer...")
                time.sleep(10)
                continue
                
        except Exception as e:
            print(f"Error fetching {ticker} from {url}: {str(e)}")
            continue
    
    return 'N/A'

def parse_website_from_content(html_content, ticker):
    """Parse the website URL from Yahoo Finance HTML content"""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Method 1: Look for https://www.apple.com pattern in the text
        # Yahoo Finance often shows the website directly in the company info
        text_content = soup.get_text()
        
        # Search for website patterns in the text
        import re
        
        # Look for website URLs in the page text
        website_patterns = [
            r'https?://www\.[\w\-\.]+\.[\w]{2,}',
            r'https?://[\w\-\.]+\.[\w]{2,}',
            r'www\.[\w\-\.]+\.[\w]{2,}',
        ]
        
        for pattern in website_patterns:
            matches = re.findall(pattern, text_content)
            for match in matches:
                # Filter out Yahoo and other non-company URLs
                if not any(skip in match.lower() for skip in [
                    'yahoo', 'finance.yahoo', 'yimg', 'javascript', 'mailto',
                    'facebook', 'twitter', 'linkedin', 'youtube', 'instagram'
                ]):
                    # This looks like a company website
                    return match
        
        # Method 2: Look for links with external domains
        links = soup.find_all('a', href=True)
        for link in links:
            href = link.get('href', '').strip()
            
            if (href and 
                href.startswith(('http://', 'https://')) and 
                len(href) > 15 and
                not any(skip in href.lower() for skip in [
                    'yahoo', 'finance.yahoo', 'yimg.com', 'javascript:', 'mailto:',
                    'facebook.com', 'twitter.com', 'linkedin.com', 'youtube.com', 'instagram.com',
                    'sec.gov', 'edgar', '#', '?', 'news.yahoo', 'sports.yahoo'
                ])):
                
                # Check if this looks like a company website
                domain = urlparse(href).netloc.lower()
                if domain and '.' in domain:
                    return href
        
        # Method 3: Look for specific text patterns that indicate website info
        # Search for text like "Website:" or "Company Website:"
        website_indicators = soup.find_all(text=re.compile(r'website|web site', re.IGNORECASE))
        for indicator in website_indicators:
            parent = indicator.parent
            if parent:
                # Look for links near the website indicator
                nearby_links = parent.find_all('a', href=True)
                for link in nearby_links:
                    href = link.get('href', '')
                    if href.startswith('http') and not 'yahoo' in href.lower():
                        return href
        
        return 'N/A'
        
    except Exception as e:
        print(f"Error parsing content for {ticker}: {str(e)}")
        return 'N/A'

def main():
    """Main function"""
    
    # File names
    ticker_file = "tickers.csv"  # Updated to look for CSV first
    if not os.path.exists(ticker_file):
        ticker_file = "tickers.txt"  # Fallback to txt
    
    output_file = "company_domains.csv"
    
    # Check if ticker file exists
    if not os.path.exists(ticker_file):
        print(f"❌ Error: No ticker file found!")
        print("Please create either:")
        print("  • tickers.csv (with tickers in first column)")
        print("  • tickers.txt (one ticker per line)")
        print("\nExample tickers.csv:")
        print("Ticker,Company")
        print("AAPL,Apple Inc")
        print("GOOGL,Alphabet Inc")
        return
    
    # Read ticker symbols
    print(f"📖 Reading ticker symbols from {ticker_file}...")
    tickers = []
    
    try:
        if ticker_file.endswith('.csv'):
            df = pd.read_csv(ticker_file)
            # Use the first column, regardless of header
            tickers = df.iloc[:, 0].dropna().astype(str).str.strip().str.upper().tolist()
        else:
            with open(ticker_file, 'r') as f:
                tickers = [line.strip().upper() for line in f if line.strip()]
    except Exception as e:
        print(f"❌ Error reading {ticker_file}: {e}")
        return
    
    if not tickers:
        print("❌ No ticker symbols found!")
        return
    
    # Remove duplicates while preserving order
    seen = set()
    unique_tickers = []
    for ticker in tickers:
        if ticker not in seen:
            seen.add(ticker)
            unique_tickers.append(ticker)
    
    tickers = unique_tickers
    print(f"✅ Found {len(tickers)} unique ticker symbols")
    
    # Create session
    session = create_session()
    
    # Process tickers
    results = []
    success_count = 0
    failed_count = 0
    
    print(f"\n🚀 Starting to scrape {len(tickers)} companies...")
    print("⏱️  This will take approximately", len(tickers) * 5, "seconds (5 sec per ticker)")
    print("🤖 Using random delays and headers to avoid being blocked...")
    
    for i, ticker in enumerate(tickers, 1):
        print(f"\n[{i:3}/{len(tickers)}] {ticker:<8}", end=' ')
        
        # Get the website URL
        website_url = get_company_website(ticker, session)
        
        # Extract domain
        domain = extract_domain_from_url(website_url)
        
        if domain != 'N/A' and website_url != 'N/A':
            success_count += 1
            print(f"✅ {domain}")
        else:
            failed_count += 1
            print("❌ No website found")
        
        # Add to results
        results.append({
            'Ticker': ticker,
            'Website_URL': website_url,
            'Domain': domain
        })
        
        # Random delay between 3-7 seconds to avoid detection
        delay = random.uniform(3, 7)
        if i < len(tickers):  # Don't wait after the last one
            time.sleep(delay)
        
        # Print progress summary every 25 tickers
        if i % 25 == 0:
            print(f"\n📊 Progress: {i}/{len(tickers)} completed | ✅ {success_count} found | ❌ {failed_count} failed")
    
    # Save results
    print(f"\n\n💾 Saving results...")
    
    try:
        df_results = pd.DataFrame(results)
        df_results.to_csv(output_file, index=False)
        
        # Create domain-only file
        domain_only_file = "domains_only.txt"
        successful_results = [r for r in results if r['Domain'] != 'N/A']
        unique_domains = sorted(set([r['Domain'] for r in successful_results]))
        
        with open(domain_only_file, 'w') as f:
            for domain in unique_domains:
                f.write(domain + '\n')
        
        # Print final summary
        success_rate = (success_count / len(tickers)) * 100
        
        print(f"\n🎉 Scraping Complete!")
        print(f"📊 Final Results:")
        print(f"   • Total companies: {len(tickers)}")
        print(f"   • Websites found: {success_count} ({success_rate:.1f}%)")
        print(f"   • Failed to find: {failed_count}")
        print(f"   • Unique domains: {len(unique_domains)}")
        
        print(f"\n📁 Files created:")
        print(f"   • {output_file} - Complete results")
        print(f"   • {domain_only_file} - Clean domain list ({len(unique_domains)} domains)")
        
        if success_rate < 50:
            print(f"\n⚠️  Success rate is low ({success_rate:.1f}%)")
            print("This might be due to:")
            print("• Yahoo Finance blocking requests")
            print("• Invalid ticker symbols") 
            print("• Network issues")
            print("Try running with a smaller batch of tickers first")
        
    except Exception as e:
        print(f"❌ Error saving results: {e}")

if __name__ == "__main__":
    main()
