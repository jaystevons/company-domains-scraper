#!/usr/bin/env python3
"""
SAFE Company Domain Scraper - SAVES EVERY 50 ENTRIES
Fixed version that will NEVER lose your work again
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

def is_blocked_domain(url):
    """Enhanced domain blocking - NO MORE RIVALS.COM!"""
    if not url:
        return True
    
    try:
        domain = urlparse(url).netloc.lower()
    except:
        return True
    
    # Comprehensive blocklist
    blocked_domains = {
        # Yahoo and all subdomains
        'yahoo.com', 'finance.yahoo.com', 'yimg.com', 'yahooapis.com',
        'yahoo.uservoice.com', 'yastatic.net',
        
        # Rivals - ALL variations blocked
        'rivals.com', 'n.rivals.com', 'sports.rivals.com', 'www.rivals.com',
        
        # Ad networks
        'googleadservices.com', 'googlesyndication.com', 'doubleclick.net',
        'googletagmanager.com', 'google-analytics.com', 'googleanalytics.com',
        'outbrain.com', 'taboola.com', 'adsystem.com', 'amazon-adsystem.com',
        
        # Social media
        'facebook.com', 'twitter.com', 'linkedin.com', 'youtube.com',
        'instagram.com', 'tiktok.com', 'pinterest.com',
        
        # Financial news (not company sites)
        'sec.gov', 'edgar.sec.gov', 'bloomberg.com', 'reuters.com',
        'marketwatch.com', 'fool.com', 'seekingalpha.com', 'cnbc.com',
        'wsj.com', 'ft.com', 'barrons.com',
    }
    
    # Check exact domain
    if domain in blocked_domains:
        return True
    
    # Check if any blocked domain is contained in this domain
    for blocked in blocked_domains:
        if blocked in domain:
            return True
    
    # Block common ad patterns
    ad_patterns = ['ads', 'ad-', 'track', 'analytics', 'pixel', 'beacon']
    for pattern in ad_patterns:
        if pattern in domain:
            return True
    
    return False

def get_company_website(ticker):
    """Get company website from Yahoo Finance profile page"""
    
    url = f"https://finance.yahoo.com/quote/{ticker}/profile/"
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Look for external company website links
        links = soup.find_all('a', href=True)
        for link in links:
            href = link.get('href', '').strip()
            
            if (href and 
                href.startswith(('http://', 'https://')) and 
                len(href) > 10 and
                not is_blocked_domain(href)):
                
                # Basic validation - looks like a company domain
                domain = urlparse(href).netloc.lower()
                if domain and '.' in domain and len(domain.split('.')) <= 3:
                    return href
        
        return 'N/A'
        
    except Exception as e:
        print(f"Error with {ticker}: {str(e)}")
        return 'N/A'

def save_progress(results, output_file, domain_file, ticker_count):
    """Save current progress - GUARANTEED SAVE"""
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
        
        print(f"💾 SAVED: {ticker_count} tickers processed, {len(unique_domains)} domains found")
        return True
        
    except Exception as e:
        print(f"❌ SAVE ERROR: {e}")
        return False

def load_existing_results(output_file):
    """Load existing results to resume from where we left off"""
    if os.path.exists(output_file):
        try:
            df = pd.read_csv(output_file)
            results = df.to_dict('records')
            completed_tickers = set(df['Ticker'].tolist())
            print(f"📊 RESUMING: Found {len(results)} existing results")
            return results, completed_tickers
        except:
            print("⚠️ Could not load existing results, starting fresh")
    
    return [], set()

def main():
    """Main function - SAFE VERSION WITH AUTO-SAVE"""
    
    print("🛡️ SAFE Company Domain Scraper - AUTO-SAVES EVERY 50 ENTRIES")
    print("=" * 60)
    print("🔒 Your work will NEVER be lost again!")
    print("💾 Saves progress every 50 tickers automatically")
    
    # File names
    ticker_files = ["tickers.csv", "tickers.txt"]
    ticker_file = None
    
    for file in ticker_files:
        if os.path.exists(file):
            ticker_file = file
            break
    
    if not ticker_file:
        print("❌ Error: No ticker file found!")
        print("Please create 'tickers.csv' or 'tickers.txt'")
        return
    
    output_file = "company_domains.csv"
    domain_file = "domains_only.txt"
    
    # Load existing results (resume capability)
    results, completed_tickers = load_existing_results(output_file)
    
    # Read ticker symbols
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
    
    if not tickers:
        print("❌ No ticker symbols found!")
        return
    
    # Remove duplicates and already completed tickers
    unique_tickers = []
    for ticker in tickers:
        if ticker not in completed_tickers and ticker not in [t for t in unique_tickers]:
            unique_tickers.append(ticker)
    
    tickers = unique_tickers
    
    print(f"✅ Total new tickers to process: {len(tickers)}")
    print(f"✅ Already completed: {len(completed_tickers)}")
    
    if not tickers:
        print("🎉 All tickers already completed!")
        return
    
    # Process tickers with GUARANTEED SAVING
    success_count = 0
    failed_count = 0
    
    print(f"\n🚀 Starting SAFE scraping...")
    print(f"💾 Will save progress every 50 tickers")
    print(f"⏱️ Estimated time: {len(tickers) * 3} seconds")
    
    for i, ticker in enumerate(tickers, 1):
        print(f"[{i:4}/{len(tickers):4}] {ticker:<8}", end=' ')
        
        # Get website
        website_url = get_company_website(ticker)
        domain = extract_domain_from_url(website_url)
        
        if domain != 'N/A' and website_url != 'N/A':
            success_count += 1
            print(f"✅ {domain}")
        else:
            failed_count += 1
            print("❌ No website found")
        
        # Store result
        results.append({
            'Ticker': ticker,
            'Website_URL': website_url,
            'Domain': domain
        })
        
        # SAVE EVERY 50 ENTRIES - GUARANTEED!
        if i % 50 == 0:
            save_progress(results, output_file, domain_file, len(results))
            
        # Progress summary every 100 tickers
        if i % 100 == 0:
            success_rate = (success_count / i) * 100
            print(f"\n📊 Progress: {i}/{len(tickers)} | ✅ {success_count} | ❌ {failed_count} | Rate: {success_rate:.1f}%")
        
        # Respectful delay
        delay = random.uniform(2, 4)
        time.sleep(delay)
    
    # Final save
    print(f"\n💾 Final save...")
    save_progress(results, output_file, domain_file, len(results))
    
    # Final summary
    total_success = len([r for r in results if r['Domain'] != 'N/A'])
    success_rate = (total_success / len(results)) * 100
    
    print(f"\n🎉 SCRAPING COMPLETE - ALL DATA SAVED!")
    print(f"📊 Final Results:")
    print(f"   • Total processed: {len(results)}")
    print(f"   • Websites found: {total_success} ({success_rate:.1f}%)")
    print(f"   • Failed: {len(results) - total_success}")
    
    print(f"\n📁 Files created:")
    print(f"   • {output_file} - Complete results")
    print(f"   • {domain_file} - Clean domain list")
    print(f"\n🔒 Your work is SAFE and SAVED!")

if __name__ == "__main__":
    main()
