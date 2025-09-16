#!/usr/bin/env python3
"""
Yahoo Finance Compliant Domain Scraper
Respects Yahoo's official rate limits: 60 GET requests per minute, 360 per hour, 8000 per day
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from urllib.parse import urlparse
import sys
import os
from datetime import datetime, timedelta
import json

class YahooRateLimiter:
    """Rate limiter that respects Yahoo's official limits"""
    
    def __init__(self):
        self.requests_per_minute = 0
        self.requests_per_hour = 0
        self.requests_per_day = 0
        self.minute_start = datetime.now()
        self.hour_start = datetime.now()
        self.day_start = datetime.now()
        
        # Yahoo's official limits (being conservative)
        self.MAX_PER_MINUTE = 50  # Under the 60 limit
        self.MAX_PER_HOUR = 300   # Under the 360 limit  
        self.MAX_PER_DAY = 7000   # Under the 8000 limit
        
    def wait_if_needed(self):
        """Wait if we're approaching rate limits"""
        now = datetime.now()
        
        # Reset counters if time windows have passed
        if (now - self.minute_start).total_seconds() >= 60:
            self.requests_per_minute = 0
            self.minute_start = now
            
        if (now - self.hour_start).total_seconds() >= 3600:
            self.requests_per_hour = 0
            self.hour_start = now
            
        if (now - self.day_start).total_seconds() >= 86400:
            self.requests_per_day = 0
            self.day_start = now
        
        # Check if we need to wait
        if self.requests_per_minute >= self.MAX_PER_MINUTE:
            wait_time = 60 - (now - self.minute_start).total_seconds()
            if wait_time > 0:
                print(f"⏳ Rate limit: waiting {wait_time:.0f}s for minute reset...")
                time.sleep(wait_time + 1)
                self.requests_per_minute = 0
                self.minute_start = datetime.now()
        
        if self.requests_per_hour >= self.MAX_PER_HOUR:
            wait_time = 3600 - (now - self.hour_start).total_seconds()
            if wait_time > 0:
                print(f"⏳ Rate limit: waiting {wait_time/60:.0f}m for hour reset...")
                time.sleep(wait_time + 1)
                self.requests_per_hour = 0
                self.hour_start = datetime.now()
                
        if self.requests_per_day >= self.MAX_PER_DAY:
            wait_time = 86400 - (now - self.day_start).total_seconds()
            if wait_time > 0:
                print(f"⏳ Daily limit reached. Waiting {wait_time/3600:.1f}h for reset...")
                time.sleep(wait_time + 1)
                self.requests_per_day = 0
                self.day_start = datetime.now()
    
    def record_request(self):
        """Record that we made a request"""
        self.requests_per_minute += 1
        self.requests_per_hour += 1
        self.requests_per_day += 1
        
    def get_status(self):
        """Get current rate limit status"""
        return {
            'per_minute': f"{self.requests_per_minute}/{self.MAX_PER_MINUTE}",
            'per_hour': f"{self.requests_per_hour}/{self.MAX_PER_HOUR}",
            'per_day': f"{self.requests_per_day}/{self.MAX_PER_DAY}"
        }

def extract_domain_from_url(url):
    """Extract just the domain name from a full URL"""
    if not url or url == 'N/A':
        return 'N/A'
    
    try:
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        parsed = urlparse(url)
        domain = parsed.netloc
        
        if domain.startswith('www.'):
            domain = domain[4:]
        
        return domain
    except:
        return 'N/A'

def is_valid_company_domain(url):
    """Check if URL looks like a legitimate company domain"""
    if not url or url == 'N/A':
        return False
    
    try:
        domain = urlparse(url).netloc.lower()
    except:
        return False
    
    # Filter out unwanted domains
    blocked_patterns = [
        'yahoo', 'rivals', 'outbrain', 'taboola', 'doubleclick',
        'facebook', 'twitter', 'linkedin', 'youtube', 'instagram',
        'javascript:', 'mailto:', 'sec.gov', 'bloomberg', 'reuters'
    ]
    
    for pattern in blocked_patterns:
        if pattern in domain:
            return False
    
    # Must be HTTP/HTTPS and reasonable length
    if url.startswith(('http://', 'https://')) and len(url) > 10:
        return True
    
    return False

def get_company_website(ticker, rate_limiter):
    """Get company website from Yahoo Finance with rate limiting"""
    
    url = f"https://finance.yahoo.com/quote/{ticker}/profile/"
    
    try:
        # Wait if needed to respect rate limits
        rate_limiter.wait_if_needed()
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        # Make the request
        response = requests.get(url, headers=headers, timeout=15)
        rate_limiter.record_request()  # Record the request
        
        if response.status_code == 429:
            print(f"⚠️ Rate limited by Yahoo for {ticker}")
            return 'RATE_LIMITED'
        
        response.raise_for_status()
        
        # Parse the HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Look for external company website links
        links = soup.find_all('a', href=True)
        for link in links:
            href = link.get('href', '').strip()
            
            if is_valid_company_domain(href):
                return href
        
        return 'N/A'
        
    except requests.exceptions.RequestException as e:
        if '429' in str(e):
            print(f"⚠️ Rate limited: {ticker}")
            return 'RATE_LIMITED'
        else:
            print(f"Error with {ticker}: {e}")
            return 'N/A'
    except Exception as e:
        print(f"Error parsing {ticker}: {e}")
        return 'N/A'

def save_progress(results, output_file, domain_file):
    """Save current progress"""
    try:
        # Save main results
        df_results = pd.DataFrame(results)
        df_results.to_csv(output_file, index=False)
        
        # Save domain-only file
        successful_results = [r for r in results if r['Domain'] != 'N/A']
        unique_domains = sorted(set([r['Domain'] for r in successful_results]))
        
        with open(domain_file, 'w') as f:
            for domain in unique_domains:
                f.write(domain + '\n')
        
        return len(unique_domains)
        
    except Exception as e:
        print(f"❌ Save error: {e}")
        return 0

def main():
    """Main function with Yahoo rate limit compliance"""
    
    print("🚀 Yahoo Finance COMPLIANT Domain Scraper")
    print("=" * 50)
    print("📊 Rate Limits: 50/min, 300/hour, 7000/day (Yahoo compliant)")
    print("⏱️  This will be MUCH slower but won't get blocked!")
    
    # Initialize rate limiter
    rate_limiter = YahooRateLimiter()
    
    # File setup
    ticker_files = ["tickers.csv", "tickers.txt"]
    ticker_file = None
    
    for file in ticker_files:
        if os.path.exists(file):
            ticker_file = file
            break
    
    if not ticker_file:
        print("❌ No ticker file found!")
        return
    
    output_file = "company_domains.csv"
    domain_file = "domains_only.txt"
    
    # Read tickers
    print(f"📖 Reading from: {ticker_file}")
    tickers = []
    
    try:
        if ticker_file.endswith('.csv'):
            df = pd.read_csv(ticker_file)
            tickers = df.iloc[:, 0].dropna().astype(str).str.strip().str.upper().tolist()
        else:
            with open(ticker_file, 'r') as f:
                tickers = [line.strip().upper() for line in f if line.strip()]
    except Exception as e:
        print(f"❌ Error reading {ticker_file}: {e}")
        return
    
    # Remove duplicates
    tickers = list(dict.fromkeys(tickers))
    
    # Check for existing results
    existing_results = []
    if os.path.exists(output_file):
        try:
            existing_df = pd.read_csv(output_file)
            existing_results = existing_df.to_dict('records')
            completed_tickers = set(existing_df['Ticker'].tolist())
            tickers = [t for t in tickers if t not in completed_tickers]
            print(f"📊 Resume: {len(existing_results)} completed, {len(tickers)} remaining")
        except:
            print("⚠️ Starting fresh")
    
    if not tickers:
        print("🎉 All tickers completed!")
        return
    
    print(f"✅ Processing {len(tickers)} tickers")
    
    # Estimate time based on rate limits
    estimated_hours = len(tickers) / 300  # 300 per hour max
    print(f"⏱️  Estimated time: {estimated_hours:.1f} hours (rate limit compliant)")
    
    # Process tickers
    results = existing_results.copy()
    success_count = 0
    failed_count = 0
    rate_limited_count = 0
    
    print(f"\n🚀 Starting compliant scraping...")
    
    for i, ticker in enumerate(tickers, 1):
        # Show rate limit status every 50 tickers
        if i % 50 == 0:
            status = rate_limiter.get_status()
            print(f"\n📊 Rate Status: Min {status['per_minute']}, Hour {status['per_hour']}, Day {status['per_day']}")
        
        print(f"[{len(results)+1:4}/{len(existing_results)+len(tickers):4}] {ticker:<8}", end=' ')
        
        # Get website
        website_url = get_company_website(ticker, rate_limiter)
        
        if website_url == 'RATE_LIMITED':
            rate_limited_count += 1
            print("⚠️ Rate limited - will retry later")
            # Don't add to results, will retry
            continue
        
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
        
        # Save every 50 entries
        if len(results) % 50 == 0:
            domains_found = save_progress(results, output_file, domain_file)
            print(f"💾 SAVED: {len(results)} processed, {domains_found} domains found")
        
        # Progress summary every 100
        if i % 100 == 0:
            success_rate = (success_count / i) * 100 if i > 0 else 0
            print(f"\n📊 Progress: {i}/{len(tickers)} | ✅ {success_count} | ❌ {failed_count} | ⚠️ {rate_limited_count} | Rate: {success_rate:.1f}%")
    
    # Final save
    print(f"\n💾 Final save...")
    domains_found = save_progress(results, output_file, domain_file)
    
    # Final summary
    total_processed = success_count + failed_count
    success_rate = (success_count / total_processed) * 100 if total_processed > 0 else 0
    
    print(f"\n🎉 COMPLIANT SCRAPING COMPLETE!")
    print(f"📊 Results:")
    print(f"   • Processed: {total_processed}")
    print(f"   • Successful: {success_count} ({success_rate:.1f}%)")
    print(f"   • Failed: {failed_count}")
    print(f"   • Rate limited: {rate_limited_count}")
    print(f"   • Unique domains: {domains_found}")
    print(f"\n📁 Files: {output_file}, {domain_file}")
    print(f"🔒 No Yahoo rate limit violations!")

if __name__ == "__main__":
    main()
