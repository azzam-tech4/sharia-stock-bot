import os
import re
import logging
import time
from io import BytesIO
from requests.exceptions import ConnectionError, HTTPError
from dotenv import load_dotenv
from deep_translator import GoogleTranslator
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    BotCommand,
    BotCommandScopeDefault,
    BotCommandScopeChat
)
from telegram.constants import ParseMode, ChatAction
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
import yfinance as yf
import math
import asyncio
import pandas as pd
# --- مكتبات جديدة لكشط الويب ---
import requests
from bs4 import BeautifulSoup

# --- استيراد المكونات الجديدة ---
import db_handler as db
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import arabic_reshaper
from bidi.algorithm import get_display

# --- الثوابت والقواميس ---
SECTOR_MANUAL_TRANSLATE = { "Internet Content & Information": "محتوى الإنترنت والمعلومات", "Financial Services": "الخدمات المالية", "Asset Management": "إدارة الأصول", "Insurance - Life": "تأمين على الحياة", "Tobacco": "التبغ", "Banks": "البنوك", "Alcohol": "الخمور", "Gambling": "المقامرة", "Pork": "لحوم الخنزير", "Consumer Defensive": "الدفاع الاستهلاكي", "Semiconductors": "أشباه الموصلات", "Software - Infrastructure": "البرمجيات - البنية التحتية", "Software - Application": "البرمجيات - التطبيقات", "Biotechnology": "التكنولوجيا الحيوية", "Pharmaceuticals": "المستحضرات الصيدلانية", "Beverages - Brewers": "المشروبات - البيرة", "Resorts & Casinos": "المنتجعات والكازينوهات", "Entertainment": "ترفيه", "Beverages - Non-Alcoholic": "المشروبات - غير الكحولية",}
TRANSLATION_CACHE = {}
ADMIN_CHAT_IDS = [7567496609, 649684756]
HARAM_KEYWORDS = ["الخمور", "الخمر", "تداول الديون", "السندات", "المقامره", "المقامرة", "القمار", "البنوك", "البنك", "التبغ", "لحوم الخنزير", "الخنزير", "شركات المجون", "الأفلام الخليعة", "شركات الأفلام الخليعة", "التأمين", "تأمين الحياة", "تأمين على الحياة", "الخدمات المالية", "إدارة الأصول", "المشروبات", "البيرة", "المنتجعات", "الكازينوهات", "ترفيه", "الترفيه والتسلية", "فراغ", "alcohol", "liquor", "brewery", "wine", "pork", "swine", "gambling", "casino", "betting", "lottery", "banks", "bank", "porn", "pornography", "adult", "erotic", "bond", "debt", "insurance", "life insurance", "financial services", "asset management", "assets management", "tobacco", "beverages", "brewers", "resorts & casinos", "resorts", "casinos", "entertainment", "Leisure & Entertainment", "Leisure"]
def is_haram_activity(sector, subsector):
    if subsector and "non-alcoholic" in subsector.lower(): return False
    text = f"{sector or ''} {subsector or ''}".lower()
    for word in HARAM_KEYWORDS:
        if word.lower() in text: return True
    return False
SAR_EXCHANGE_RATE = 3.75
MESSAGES = { "en": { "choose_lang": "Please choose your language:", "lang_set": "✅ Language set to English.\n\n👋 Hello {user_mention}! Send me a stock symbol (e.g. GOOG or AAPL).", "start": "👋 Hello {user_mention}! Send me a stock symbol (e.g. GOOG or AAPL).", "searching": "Searching for {sym}... ⏳", "not_found": "⚠️ The symbol '{sym}' is not supported.", "error": "❗ An unexpected error occurred while fetching data for '{sym}':\n{err}", "rate_limit": "Please wait {delta} seconds before trying again.", "help": ("/start – Start bot\n" "/lang  – Change language\n" "/help  – Show help\n\n" "Usage:\n" "1. First send /start and choose your language.\n" "2. Then send a stock symbol to get its full results.\n" "You don’t need to send /start again each time."), "header": "📈 Shariah status for {company} ({sym}):", "sector": "• Sector: {sec}", "subsector": "• Sub-sector: {sub}", "financial_report_header": "📊 Financial Report for {company} ({sym}):", "compliance_statuses": {"compliant": "Sharia-compliant ✅", "non_compliant": "Not Sharia-compliant ❌", "haram_activity": "Not Sharia-compliant ❌", "unknown": "Unknown ❓"}, "not_available": "N/A", "report_date": "• Report Date: {date}", "purification_ratio_display": "• Purification Ratio: {ratio}", "purification_mixed_text": " (Mixed)", "show_financial_report_button": "📊 Show Financial Report", "calculate_purification_button": "🧮 Purification Calculator", "choose_profit_type": "Please choose the type of profit for {sym}:", "profit_type_capital_gains": "Capital Gains (Sale Profit)", "profit_type_dividends": "Dividends (Profit Distributions)", "enter_profit_amount": "Please enter the {profit_type} amount for {sym} (e.g., 1000 or 50.5):", "purification_result_capital_gains": ("For your capital gains of {amount} from {company} ({sym}), the amount to purify is: {purified_amount_usd:.2f} $\n" "This is equivalent to Saudi Riyals: {purified_amount_sar:.2f} SR\n\n" "You can pay the purification amount on Ehsan platform via this link: https://ehsan.sa/stockspurification"), "purification_result_dividends": ("For your dividends of {amount} from {company} ({sym}), the amount to purify is: {purified_amount_usd:.2f} $\n" "This is equivalent to Saudi Riyals: {purified_amount_sar:.2f} SR\n\n" "You can pay the purification amount on Ehsan platform via this link: https://ehsan.sa/stockspurification"), "invalid_amount": "Invalid amount. Please enter a numerical value (e.g., 1000 or 50.5).", "data_expired": "Purification/financial data for '{sym}' is not found or expired. Please search for the stock again by sending its symbol.", "purification_not_available": "Calculation is not available for '{sym}' as its purification ratio is not provided.", "command_start_desc": "Start bot / بدأ البوت", "command_lang_desc": "Change language / تغير اللغة", "command_help_desc": "Show help / عرض المساعدة", "command_broadcast_text_desc": "Broadcast text message to all users", "command_broadcast_photo_desc": "Broadcast photo to all users", "command_broadcast_video_desc": "Broadcast video to all users", "command_stats_desc": "Show bot statistics (Admin)", "disclaimer_message": "\n\n<b>⚠️ Important Disclaimer! ⚠️</b>\n<b>To disclaim responsibility before God for any error in Sharia calculation, we have strived as much as possible to obtain data accurately, but you remain solely responsible for your decision to invest or own the stock.</b>", "purification_not_allowed": "Sorry, calculation is not allowed for '{sym}' as it is not Shariah-compliant.", "purification_unavailable_for_calc": "Sorry, purification calculation is not available for '{sym}' as its data is not provided.", "share_bot_button": "➡️ Share Bot", "connection_error": "⚠️ Could not connect to data server. Please try again later.", "not_authorized_admin": "You are not authorized to use this command.", "broadcast_text_usage": "Please reply to this message with the text you want to broadcast.", "broadcast_photo_usage": "Please reply to this message with the photo you want to broadcast.", "broadcast_video_usage": "Please reply to this message with the video you want to broadcast.", "broadcast_started": "Starting broadcast to {count} users...", "broadcast_text_sent_summary": "Text broadcast sent successfully to {sent_count} users.\nFailed to send to {failed_count} users.", "broadcast_media_sent_summary": "Media broadcast sent successfully to {sent_count} users.\nFailed to send to {failed_count} users.", "no_media_found": "No photo or video found in your message.", "no_text_found_for_broadcast": "No text found in your message.", "market_cap_update_label": "Last Updated", }, "ar": { "choose_lang": "اختر لغتك:", "lang_set": "✅ تم اختيار اللغة العربية.\n\n👋 أهلاً بك يا {user_mention}! أرسل رمز السهم (مثلاً GOOG أو AAPL).", "start": "👋 أهلاً بك يا {user_mention}! أرسل رمز السهم (مثلاً GOOG أو AAPL).", "searching": "جاري البحث عن {sym}... ⏳", "not_found": "⚠️ السهم '{sym}' غير مدعوم.", "error": "❗ حدث خطأ غير متوقع أثناء جلب البيانات للسهم '{sym}':\n{err}", "rate_limit": "يرجى الانتظار {delta} ثانية وإرسال طلبك مرة أخرى.", "help": ("/start – ابدأ البوت\n" "/lang  – غيّر اللغة\n" "/help  – عرض المساعدة\n\n" "طريقة الاستخدام:\n" "1. في المرة الأولى أرسل /start واختر اللغة.\n" "2. بعدها أرسل رمز السهم لتحصل على نتائجه كاملة.\n" "لا تحتاج لإعادة /start في كل مرة."), "header": "📈 شرعية سهم {company} ({sym}):", "sector": "• القطاع: {sec}", "subsector": "• القطاع الفرعي: {sub}", "financial_report_header": "📊 التقرير المالي لسهم {company} ({sym}):", "compliance_statuses": {"compliant": "متوافق مع الضوابط الشرعية ✅", "non_compliant": "غير متوافق مع الضوابط الشرعية ❌", "haram_activity": "نشاط الشركة غير متوافق مع الضوابط الشرعية ❌", "unknown": "غير محدد ❓"}, "not_available": "غير متوفر", "report_date": "• تاريخ التقرير: {date}", "purification_ratio_display": "• نسبة التطهير: {ratio}", "purification_mixed_text": " (مختلط)", "show_financial_report_button": "📊 عرض التقرير المالي", "calculate_purification_button": "🧮 حاسبة التطهير", "choose_profit_type": "الرجاء اختيار نوع الربح لسهم {sym}:", "profit_type_capital_gains": "أرباح بيع (أرباح رأسمالية)", "profit_type_dividends": "توزيعات أرباح", "enter_profit_amount": "الرجاء إدخال مبلغ {profit_type} لسهم {sym} (مثلاً 1000 أو 50.5):", "purification_result_capital_gains": ("لربحك الرأسمالي البالغ {amount} من سهم {company} ({sym})، المبلغ الواجب تطهيره هو: {purified_amount_usd:.2f} $\n" "وهذا يعادل بالريال السعودي: {purified_amount_sar:.2f} SR\n\n" "بالإمكان دفع قيمة التطهير على موقع إحسان من خلال الرابط: https://ehsan.sa/stockspurification"), "purification_result_dividends": ("لتوزيعات أرباحك البالغة {amount} من سهم {company} ({sym})، المبلغ الواجب تطهيره هو: {purified_amount_usd:.2f} $\n" "وهذا يعادل بالريال السعودي: {purified_amount_sar:.2f} SR\n\n" "بالإمكان دفع قيمة التطهير على موقع إحسان من خلال الرابط: https://ehsan.sa/stockspurification"), "invalid_amount": "مبلغ غير صالح. الرجاء إدخال قيمة رقمية (مثلاً 1000 أو 50.5).", "data_expired": "بيانات التقرير منتهية الصلاحية أو غير موجودة. يرجى البحث عن السهم مرة أخرى بإرسال رمزه.", "disclaimer_message": "\n<b>⚠️ تنويه مهم جداً! ⚠️</b>\n<b>لا نتحمل المسؤولية أمام الله عن أي خطأ في احتساب الشرعيه اجتهدنا قدر المستطاع في الحصول علي قواعد البيانات بدقة لكن تبقي انت المسؤول عن قرار استثمارك او تملكك للسهم</b>", "purification_not_allowed": "عذراً، لا يمكن حساب التطهير لسهم '{sym}' لأنه غير شرعي.", "purification_unavailable_for_calc": "عذراً، لا يمكن حساب التطهير لسهم '{sym}' لأنه غير متوفر.", "share_bot_button": "➡️ مشاركة البوت", "connection_error": "⚠️ تعذر الاتصال بخادم البيانات. يرجى المحاولة لاحقاً.", "not_authorized_admin": "غير مصرح لك باستخدام هذا الأمر.", "broadcast_text_usage": "الرجاء الرد على هذه الرسالة بالنص الذي تريد نشره.", "broadcast_photo_usage": "الرجاء الرد على هذه الرسالة بالصورة التي تريد نشرها.", "broadcast_video_usage": "الرجاء الرد على هذه الرسالة بالفيديو الذي تريد نشره.", "broadcast_started": "جاري بدء الإرسال إلى {count} مستخدم...", "broadcast_text_sent_summary": "تم إرسال الرسالة النصية بنجاح إلى {sent_count} مستخدم.\nفشل الإرسال إلى {failed_count} مستخدم.", "broadcast_media_sent_summary": "تم إرسال الميديا بنجاح إلى {sent_count} مستخدم.\nفشل الإرسال إلى {failed_count} مستخدم.", "no_media_found": "لم يتم العثور على صورة أو فيديو في رسالتك.", "no_text_found_for_broadcast": "لم يتم العثور على نص في رسالتك.", "command_start_desc": "ابدا البوت", "command_lang_desc": "غير اللغة", "command_help_desc": "عرض المساعدة", "command_broadcast_text_desc": "نشر رسالة نصية لجميع المستخدمين", "command_broadcast_photo_desc": "نشر صورة لجميع المستخدمين", "command_broadcast_video_desc": "نشر فيديو لجميع المستخدمين", "command_stats_desc": "عرض إحصائيات البوت (للمسؤول)", "market_cap_update_label": "آخر تحديث", },}
BANK_NAMES = { "بنك البلاد": {"en": "Bank Albilad", "ar": "بنك البلاد"}, "بنك الراجحي": {"en": "Al Rajhi Bank", "ar": "بنك الراجحي"},}

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
RATE_LIMIT_SECONDS = 5
CACHE_TTL = 3600

def manual_or_translate(text, lang_to):
    if not text or not isinstance(text, str): return text
    key = text.strip()
    if lang_to == "ar" and key in SECTOR_MANUAL_TRANSLATE: return SECTOR_MANUAL_TRANSLATE[key]
    cache_key = f"{key}-{lang_to}"
    if cache_key in TRANSLATION_CACHE: return TRANSLATION_CACHE[cache_key]
    try:
        translator = GoogleTranslator(source="auto", target=lang_to)
        translated_text = translator.translate(text)
        TRANSLATION_CACHE[cache_key] = translated_text
        return translated_text
    except Exception as e:
        logger.warning(f"Failed to translate '{text}' to '{lang_to}': {e}")
        return text

def nice(v, lang):
    if v is None or (isinstance(v, (float)) and math.isnan(v)): return MESSAGES[lang]["not_available"]
    if isinstance(v, (int, float)):
        if abs(v) >= 1_000_000_000: return f"{v/1_000_000_000:,.2f}B"
        elif abs(v) >= 1_000_000: return f"{v/1_000_000:,.2f}M"
        elif abs(v) >= 1_000: return f"{v/1_000:,.2f}K"
        else: return f"{v:,.2f}" if isinstance(v, float) else str(v)
    return str(v)

def _get_financial_value(dataframe, key, default=None):
    if dataframe is not None and not dataframe.empty and key in dataframe.index:
        try:
            val = dataframe.loc[key].iloc[0]
            return val if pd.notna(val) else default
        except (ValueError, TypeError, IndexError):
            return default
    return default

def fetch_interest_income_from_web(symbol):
    try:
        url = f"https://finance.yahoo.com/quote/{symbol}/financials"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        possible_labels = ['Net Interest Income', 'Interest Income']
        title_divs = soup.find_all('div', class_='Ta(c) Py(6px) Bxz(bb) BdB Bdc($seperatorColor) Miw(120px) Miw(140px)--pnclg D(tbc)')
        for title_div in title_divs:
            title_text = title_div.find('span').get_text(strip=True)
            if title_text in possible_labels:
                data_div = title_div.find_next_sibling('div', attrs={'data-test': 'fin-col'})
                if data_div:
                    value_str = data_div.text.strip().replace(',', '')
                    if value_str and value_str != '-':
                        if value_str.startswith('(') and value_str.endswith(')'):
                            value_str = '-' + value_str[1:-1]
                        return int(value_str) * 1000
        return None
    except Exception as e:
        logger.error(f"CRITICAL: Web scraping for {symbol} failed. Error: {e}")
        return None

def fetch_yfinance(symbol: str):
    ticker = None
    info = None
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
    except HTTPError as e:
        if e.response.status_code == 404: raise ValueError("not_found")
        logger.error(f"HTTPError fetching data for {symbol}: {e}"); raise ValueError(f"connection_error:{str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error during yfinance fetch for {symbol}: {e}"); raise ValueError(f"error:{str(e)}")

    if not info or not info.get("longName") and not info.get("shortName"):
        raise ValueError("not_found")
        
    quote_type = info.get('quoteType')
    if quote_type != 'EQUITY' or not info.get('sector'):
        logger.info(f"Symbol {symbol} is of an unsupported type ('{quote_type}') or has no sector. Rejecting.")
        raise ValueError("not_found")

    company_all = info.get("longName", info.get("shortName", symbol))
    sector = info.get("sector")
    subsector = info.get("industry")
    haram = is_haram_activity(sector, subsector)
    
    market_cap = info.get("marketCap")
    total_debt = info.get("totalDebt")
    total_assets = info.get("totalAssets")
    total_revenue = info.get("totalRevenue")
    interest_income = info.get("interestIncome") 
    
    try:
        if ticker:
            qf = ticker.quarterly_financials
            qbs = ticker.quarterly_balance_sheet
            
            total_revenue = _get_financial_value(qf, "Total Revenue", total_revenue)
            total_debt = _get_financial_value(qbs, "Total Debt", total_debt)
            total_assets = _get_financial_value(qbs, "Total Assets", total_assets)
            
            possible_interest_keys = ["Interest Income", "Net Interest Income", "Interest Income, Net"]
            found_in_api = False
            for key in possible_interest_keys:
                found_interest = _get_financial_value(qf, key)
                if found_interest is not None:
                    interest_income = found_interest
                    found_in_api = True
                    break
            
            if not found_in_api or interest_income is None:
                logger.info(f"Interest income for {symbol} not in API, trying web scraping...")
                scraped_interest = fetch_interest_income_from_web(symbol)
                if scraped_interest is not None:
                    logger.info(f"Successfully scraped interest income for {symbol}: {scraped_interest}")
                    interest_income = scraped_interest

    except Exception as e: 
        logger.warning(f"Error fetching quarterly financials for {symbol}: {e}")
    
    logger.info(f"--- Data for {symbol} ---")
    logger.info(f"Total Revenue: {total_revenue}")
    logger.info(f"Interest Income: {interest_income}")
    logger.info(f"Total Debt: {total_debt}")
    logger.info(f"Market Cap: {market_cap}")
    logger.info(f"Total Assets: {total_assets}")
    logger.info(f"------------------------")
         
    purification_ratio = None
    if interest_income is not None and total_revenue is not None and not (isinstance(interest_income, float) and math.isnan(interest_income)) and not (isinstance(total_revenue, float) and math.isnan(total_revenue)) and total_revenue > 0:
        purification_ratio = (abs(interest_income) / total_revenue) * 100

    def get_compliance_status(bank_name):
        try:
            if haram: 
                return "haram_activity"

            if bank_name == "Al-Rajhi":
                debt_denominator = market_cap
                debt_limit = 0.30
            elif bank_name == "Al-Bilad":
                debt_denominator = total_assets
                debt_limit = 0.333
            else:
                return "unknown" 

            rev_check_result = None
            if interest_income is not None and not (isinstance(interest_income, float) and math.isnan(interest_income)) and \
               total_revenue is not None and not (isinstance(total_revenue, float) and math.isnan(total_revenue)):
                
                if total_revenue > 0:
                    rev_check_result = (abs(interest_income) / total_revenue) < 0.05
                elif interest_income > 0:
                    rev_check_result = False
                else:
                    rev_check_result = True
            
            debt_check_result = None
            if total_debt is not None and not (isinstance(total_debt, float) and math.isnan(total_debt)) and \
               debt_denominator is not None and not (isinstance(debt_denominator, float) and math.isnan(debt_denominator)):

                if debt_denominator > 0:
                    debt_check_result = (total_debt / debt_denominator) < debt_limit
                else:
                    debt_check_result = False
            
            all_checks = [rev_check_result, debt_check_result]
            
            if False in all_checks:
                return "non_compliant"
            if None in all_checks:
                return "unknown"
            return "compliant"

        except (TypeError, ZeroDivisionError):
            return "unknown"

    bilad_status = get_compliance_status("Al-Bilad")
    rajhi_status = get_compliance_status("Al-Rajhi")

    compliance_results = [("بنك البلاد", bilad_status), ("بنك الراجحي", rajhi_status)]
    report_date = str(ticker.quarterly_financials.columns[0].date()) if 'ticker' in locals() and ticker and not ticker.quarterly_financials.empty else MESSAGES["ar"]["not_available"]
    
    return company_all, sector, subsector, compliance_results, {"market_cap": market_cap, "total_revenue": total_revenue, "total_debt": total_debt, "interest_income": interest_income, "total_assets": total_assets, "purification_ratio": purification_ratio}, report_date, interest_income, total_revenue

def _build_financial_report_text(lang, company, sym, metrics_data, report_date, interest_income, total_revenue, market_cap_update_time=None):
    parts = [MESSAGES[lang]["financial_report_header"].format(company=company, sym=sym)]
    financial_metrics_config = {"market_cap": {"ar": "القيمة السوقية", "en": "Market Cap"}, "total_revenue": {"ar": "مجموع الإيرادات", "en": "Total Revenue"}, "total_debt": {"ar": "إجمالي الديون", "en": "Total Debt"}, "interest_income": {"ar": "الدخل من الفوائد", "en": "Interest Income"}, "interest_income_ratio": {"ar": "الدخل من الفوائد/مجموع الإيرادات", "en": "Interest Income/Total Revenue"}, "total_debt_market_cap_ratio": {"ar": "مجموع الديون/القيمة السوقية", "en": "Total Debt/Market Cap"}, "total_assets": {"ar": "إجمالي الأصول", "en": "Total Assets"}, "debt_to_assets_ratio": {"ar": "نسبة الدين إلى الأصل", "en": "Debt to Assets Ratio"}}
    
    def get_formatted_value(key, value, lang):
        if key == "interest_income_ratio":
            if interest_income is not None and total_revenue is not None and not (isinstance(interest_income, float) and math.isnan(interest_income)) and not (isinstance(total_revenue, float) and math.isnan(total_revenue)) and total_revenue > 0: return f"{abs(interest_income)/total_revenue:.2%}"
            return MESSAGES[lang]["not_available"]
        elif key == "total_debt_market_cap_ratio":
            if metrics_data.get("total_debt") is not None and metrics_data.get("market_cap", 0) > 0: return f"{metrics_data['total_debt']/metrics_data['market_cap']:.2%}"
            return MESSAGES[lang]["not_available"]
        elif key == "debt_to_assets_ratio":
            if metrics_data.get("total_debt") is not None and metrics_data.get("total_assets", 0) > 0: return f"{metrics_data['total_debt']/metrics_data['total_assets']:.2%}"
            return MESSAGES[lang]["not_available"]
        else: return nice(value, lang)
        
    for key, names in financial_metrics_config.items():
        parts.append(f"• {names[lang]}: {get_formatted_value(key, metrics_data.get(key), lang)}")
        
    parts.append(MESSAGES[lang]["report_date"].format(date=report_date))
    
    if market_cap_update_time:
        parts.append(f"• {MESSAGES[lang]['market_cap_update_label']}: {market_cap_update_time}")
        
    return "\n".join(parts)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db.add_user_if_not_exists(user.id, user.first_name, user.username)
    lang = db.get_user_setting(user.id, 'language')
    if not lang:
        kb = [[InlineKeyboardButton("English", callback_data="lang:en"), InlineKeyboardButton("العربية", callback_data="lang:ar")]]
        await update.message.reply_text(MESSAGES["en"]["choose_lang"], reply_markup=InlineKeyboardMarkup(kb))
    else: await update.message.reply_html(MESSAGES[lang]["start"].format(user_mention=user.mention_html()))

async def lang_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = db.get_user_setting(update.effective_chat.id, 'language', 'ar')
    kb = [[InlineKeyboardButton("English", callback_data="lang:en"), InlineKeyboardButton("العربية", callback_data="lang:ar")]]
    await update.message.reply_text(MESSAGES[lang]["choose_lang"], reply_markup=InlineKeyboardMarkup(kb))

async def on_lang_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    _, code = q.data.split(":")
    db.set_user_setting(q.from_user.id, 'language', code)
    await q.edit_message_text(MESSAGES[code]["lang_set"].format(user_mention=q.from_user.mention_html()), parse_mode=ParseMode.HTML)

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = db.get_user_setting(update.effective_chat.id, 'language', 'ar')
    await update.message.reply_text(MESSAGES[lang]["help"], parse_mode=ParseMode.HTML)

def create_stats_image(stats: dict) -> BytesIO:
    plt.rcParams['font.family'] = 'Arial'
    def ar(text): return get_display(arabic_reshaper.reshape(str(text)))
    
    fig = plt.figure(figsize=(8, 13), dpi=150)
    fig.patch.set_facecolor('#f4f4f4')

    current_y = 0.96
    
    fig.text(0.5, current_y, ar("📊 إحصائيات البوت الحية"), ha='center', va='center', fontsize=22, weight='bold')
    current_y -= 0.1

    def draw_table_at(y_pos, height, ax_x, ax_width, title, data, col_labels, col_widths):
        fig.text(ax_x + ax_width / 2, y_pos, ar(title), ha='center', va='bottom', fontsize=15, weight='bold')
        
        ax = fig.add_axes([ax_x, y_pos - height, ax_width, height])
        ax.axis('off')
        
        table = ax.table(cellText=data, colLabels=col_labels, colWidths=col_widths, cellLoc='center', loc='center')
        table.auto_set_font_size(False)
        table.set_fontsize(11)
        table.scale(1, 1.9)
        
        for key, cell in table.get_celld().items():
            cell.set_edgecolor('w')
            if key[0] == 0 and col_labels:
                cell.set_text_props(weight='bold', color='white')
                cell.set_facecolor('#4A90E2')
            else:
                cell.set_facecolor('#FFFFFF')
                cell.set_text_props(ha='right' if key[1] == 1 else 'center')
        return height + 0.05

    user_data = [ [ar(stats['total_users']), ar("الإجمالي")], [ar(stats['active_users_today']), ar("النشطون (اليوم)")], [ar(stats['active_users_week']), ar("النشطون (أسبوع)")], [ar(stats['active_users_month']), ar("النشطون (شهر)")], [ar(stats['new_users_today']), ar("الجدد (اليوم)")], [ar(stats['new_users_week']), ar("الجدد (أسبوع)")], [ar(stats['new_users_month']), ar("الجدد (شهر)")], ]
    search_data = [ [ar(stats['total_searches']), ar("الإجمالي")], [ar(stats['searches_today']), ar("اليوم")], [ar(stats['searches_yesterday']), ar("أمس")], [ar(stats['searches_this_week']), ar("هذا الأسبوع")], [ar(stats['searches_last_week']), ar("الأسبوع الماضي")], [ar(stats['searches_this_month']), ar("هذا الشهر")], [ar(stats['searches_last_month']), ar("الشهر الماضي")], ]
    lang_data = [[ar(count), ar("العربية" if lang == 'ar' else "English")] for lang, count in stats['language_distribution'].items()] or [[ar(0), ar("لا يوجد")]]
    
    def format_stock_data(stock_list):
        if not stock_list: return [[ar("-"), ar("-")]]
        return [[ar(f"{count}"), ar(symbol)] for symbol, count in stock_list]

    h = draw_table_at(current_y, 0.22, 0.05, 0.4, "👤 المستخدمون", user_data, None, [0.4, 0.6])
    draw_table_at(current_y, 0.22, 0.55, 0.4, "🔍 عمليات البحث", search_data, None, [0.4, 0.6])
    current_y -= h
    
    h = draw_table_at(current_y, 0.1, 0.1, 0.8, "🌐 توزيع اللغات", lang_data, [ar("العدد"), ar("اللغة")], [0.4, 0.6])
    current_y -= h

    h = draw_table_at(current_y, 0.15, 0.05, 0.4, "⭐ الأكثر بحثاً (اليوم)", format_stock_data(stats['top_stocks_day']), [ar("العدد"), ar("الرمز")], [0.4, 0.6])
    draw_table_at(current_y, 0.15, 0.55, 0.4, "⭐ الأكثر بحثاً (الأسبوع)", format_stock_data(stats['top_stocks_week']), [ar("العدد"), ar("الرمز")], [0.4, 0.6])
    current_y -= h
    
    h = draw_table_at(current_y, 0.15, 0.05, 0.4, "⭐ الأكثر بحثاً (الشهر)", format_stock_data(stats['top_stocks_month']), [ar("العدد"), ar("الرمز")], [0.4, 0.6])
    draw_table_at(current_y, 0.15, 0.55, 0.4, "⭐ الأكثر بحثاً (الإجمالي)", format_stock_data(stats['top_stocks_overall']), [ar("العدد"), ar("الرمز")], [0.4, 0.6])

    buf = BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close(fig)
    return buf

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    lang = db.get_user_setting(cid, 'language', 'ar')
    if cid not in ADMIN_CHAT_IDS:
        await update.message.reply_text(MESSAGES[lang]["not_authorized_admin"]); return
    await update.message.reply_chat_action(action=ChatAction.UPLOAD_PHOTO)
    stats = db.get_bot_stats()
    try:
        image_buffer = create_stats_image(stats)
        await update.message.reply_photo(photo=image_buffer, caption=f"آخر تحديث للإحصائيات في: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as e:
        logger.error(f"Failed to generate stats image: {e}")
        await update.message.reply_text("حدث خطأ أثناء إنشاء صورة الإحصائيات. يرجى مراجعة السجلات.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid, user = update.effective_chat.id, update.effective_user
    db.add_user_if_not_exists(cid, user.first_name, user.username)
    lang = db.get_user_setting(cid, 'language', 'ar')
    user_state = db.get_user_state(cid)
    
    if cid in ADMIN_CHAT_IDS and user_state and "broadcast" in user_state.get("state", ""):
        # Broadcast logic handling...
        return

    if user_state and user_state.get("state") == "waiting_for_profit_amount":
        user_msg_text = update.message.text.strip() if update.message.text else ""
        try:
            profit_amount = float(user_msg_text)
            state_data = user_state
            db.clear_user_state(cid)
            sym, company, purification_ratio, profit_type_key = state_data["sym"], state_data["company"], state_data["purification_ratio"], state_data["profit_type_key"]
            if purification_ratio is None or math.isnan(purification_ratio):
                await update.message.reply_text(MESSAGES[lang]["purification_not_available"].format(sym=sym), parse_mode=ParseMode.HTML)
                return
            purified_amount_usd = profit_amount * (purification_ratio / 100)
            purified_amount_sar = purified_amount_usd * SAR_EXCHANGE_RATE
            await update.message.reply_text(MESSAGES[lang][f"purification_result_{profit_type_key}"].format(amount=nice(profit_amount, lang), company=company, sym=sym, purified_amount_usd=purified_amount_usd, purified_amount_sar=purified_amount_sar,), parse_mode=ParseMode.HTML)
        except ValueError:
            await update.message.reply_text(MESSAGES[lang]["invalid_amount"], parse_mode=ParseMode.HTML)
        return

    user_msg_text = update.message.text.strip() if update.message.text else ""
    cleaned_symbol = re.sub(r'[^a-zA-Z0-9.-]', '', user_msg_text)
    if not cleaned_symbol: await update.message.reply_text(MESSAGES[lang]["not_found"].format(sym=user_msg_text)); return
    sym = cleaned_symbol.upper()
    now = time.time()
    last_req_time = db.get_user_setting(cid, 'last_request_time', 0)
    delta = RATE_LIMIT_SECONDS - (now - last_req_time)
    if delta > 0: await update.message.reply_text(MESSAGES[lang]["rate_limit"].format(delta=max(1, int(delta))), parse_mode=ParseMode.HTML); return
    db.set_user_setting(cid, 'last_request_time', now)
    temp_message = await update.message.reply_text(MESSAGES[lang]["searching"].format(sym=sym))
    try:
        stock_data = db.get_cached_stock(sym, CACHE_TTL)
        if not stock_data:
            stock_data = fetch_yfinance(sym)
            db.cache_stock(sym, stock_data)
        db.log_search(cid, sym)
        company_all, sector, subsector, compliance_results, metrics_data, report_date, interest_income, total_revenue = stock_data
        match_company_name = re.search(r"[\u0600-\u06FFA-Za-z].*$", company_all)
        company = match_company_name.group(0) if match_company_name else company_all
        parts_shariah = [MESSAGES[lang]["header"].format(company=company, sym=sym)]
        if sector: parts_shariah.append(f"• {MESSAGES[lang]['sector'].split(':')[0]}: {manual_or_translate(sector, lang)}")
        if subsector: parts_shariah.append(f"• {MESSAGES[lang]['subsector'].split(':')[0]}: {manual_or_translate(subsector, lang)}")
        actual_compliance_statuses = [status_key for _, status_key in compliance_results]
        for name_ar, status_key in compliance_results: parts_shariah.append(f"- {BANK_NAMES[name_ar][lang]}: {MESSAGES[lang]['compliance_statuses'][status_key]}")
        pur_val = metrics_data.get("purification_ratio")
        pur_text = MESSAGES[lang]["not_available"]
        if "haram_activity" in actual_compliance_statuses or (all(s in ["non_compliant", "haram_activity"] for s in actual_compliance_statuses) and not any(s in ["compliant", "unknown"] for s in actual_compliance_statuses)): pur_text = "❌"
        elif pur_val is not None and not math.isnan(pur_val): pur_text = f"{pur_val:.2f}%" + MESSAGES[lang]["purification_mixed_text"]
        parts_shariah.append(MESSAGES[lang]["purification_ratio_display"].format(ratio=pur_text))
        parts_shariah.append(MESSAGES[lang]["disclaimer_message"])
        keyboard = [[InlineKeyboardButton(MESSAGES[lang]["show_financial_report_button"], callback_data=f"show_report:{sym}"), InlineKeyboardButton(MESSAGES[lang]["calculate_purification_button"], callback_data=f"calc_purify:{sym}")], [InlineKeyboardButton(MESSAGES[lang]["share_bot_button"], url=f"https://t.me/share/url?url=https://t.me/{context.bot.username}")]]
        
        db.set_report_data(
            cid,
            sym,
            {
                "lang": lang,
                "company": company,
                "sym": sym,
                "metrics_data": metrics_data,
                "report_date": report_date,
                "interest_income": interest_income,
                "total_revenue": total_revenue,
                "purification_ratio_for_calc": pur_val,
                "actual_compliance_statuses": actual_compliance_statuses,
                "market_cap_update_time": time.strftime('%Y-%m-%d')
            }
        )
        
        await temp_message.delete()
        await update.message.reply_text("\n".join(parts_shariah), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(keyboard))
    except ValueError as e:
        await temp_message.delete()
        error_msg = str(e)
        if "not_found" in error_msg: await update.message.reply_text(MESSAGES[lang]["not_found"].format(sym=sym), parse_mode=ParseMode.HTML)
        else: await update.message.reply_text(MESSAGES[lang]["error"].format(sym=sym, err=error_msg), parse_mode=ParseMode.HTML)
    except Exception as e:
        await temp_message.delete(); logger.error(f"Unexpected error for {sym}: {e}")
        await update.message.reply_text(MESSAGES[lang]["error"].format(sym=sym, err=str(e)), parse_mode=ParseMode.HTML)

async def show_financial_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    cid, sym = q.from_user.id, q.data.split(":")[-1]
    report_data = db.get_report_data(cid, sym)
    if report_data:
        lang = report_data["lang"]
        company = report_data["company"]
        metrics_data = report_data["metrics_data"]
        report_date = report_data["report_date"]
        interest_income = report_data["interest_income"]
        total_revenue = report_data["total_revenue"]
        market_cap_update_time = report_data.get("market_cap_update_time")

        await q.message.reply_text(
            _build_financial_report_text(
                lang, company, sym, metrics_data, report_date, interest_income, total_revenue, market_cap_update_time
            ),
            parse_mode=ParseMode.HTML
        )
    else:
        lang_code = db.get_user_setting(cid, 'language', 'ar')
        await q.message.reply_text(MESSAGES[lang_code]["data_expired"].format(sym=sym), parse_mode=ParseMode.HTML)


async def calculate_purification_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    cid, sym = q.from_user.id, q.data.split(":")[-1]
    report_data = db.get_report_data(cid, sym)
    if report_data:
        lang, purification_ratio, actual_compliance_statuses = report_data["lang"], report_data["purification_ratio_for_calc"], report_data.get("actual_compliance_statuses", [])
        if "haram_activity" in actual_compliance_statuses or (all(s in ["non_compliant", "haram_activity"] for s in actual_compliance_statuses) and not any(s in ["compliant", "unknown"] for s in actual_compliance_statuses)):
            await q.message.reply_text(MESSAGES[lang]["purification_not_allowed"].format(sym=sym), parse_mode=ParseMode.HTML); return
        if purification_ratio is None or math.isnan(purification_ratio):
            await q.message.reply_text(MESSAGES[lang]["purification_unavailable_for_calc"].format(sym=sym), parse_mode=ParseMode.HTML); return
        db.set_user_state(cid, {"state": "waiting_for_profit_type", "sym": sym, "company": report_data["company"], "purification_ratio": purification_ratio})
        keyboard = [[InlineKeyboardButton(MESSAGES[lang]["profit_type_capital_gains"], callback_data=f"profit_type:capital_gains:{sym}")], [InlineKeyboardButton(MESSAGES[lang]["profit_type_dividends"], callback_data=f"profit_type:dividends:{sym}")]]
        await q.message.reply_text(MESSAGES[lang]["choose_profit_type"].format(sym=sym), reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
    else: await q.message.reply_text(MESSAGES[db.get_user_setting(cid, 'language', 'ar')]["data_expired"].format(sym=sym), parse_mode=ParseMode.HTML)

async def handle_profit_type_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    cid, parts = q.from_user.id, q.data.split(":")
    profit_type_key, sym = parts[1], parts[-1]
    lang = db.get_user_setting(cid, 'language', 'ar')
    state_data = db.get_user_state(cid)
    if state_data and state_data.get('state') == "waiting_for_profit_type" and state_data.get('sym') == sym:
        state_data["state"] = "waiting_for_profit_amount"; state_data["profit_type_key"] = profit_type_key
        db.set_user_state(cid, state_data)
        await q.edit_message_text(MESSAGES[lang]["enter_profit_amount"].format(profit_type=MESSAGES[lang][f"profit_type_{profit_type_key}"], sym=sym), parse_mode=ParseMode.HTML)
    else: await q.message.reply_text(MESSAGES[lang]["data_expired"].format(sym=sym), parse_mode=ParseMode.HTML)

async def broadcast_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid, lang = update.effective_chat.id, db.get_user_setting(update.effective_chat.id, 'language', 'ar')
    if cid not in ADMIN_CHAT_IDS: await update.message.reply_text(MESSAGES[lang]["not_authorized_admin"]); return
    db.set_user_state(cid, {"state": "waiting_for_broadcast_text"}); await update.message.reply_text(MESSAGES[lang]["broadcast_text_usage"])

async def broadcast_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid, lang = update.effective_chat.id, db.get_user_setting(update.effective_chat.id, 'language', 'ar')
    if cid not in ADMIN_CHAT_IDS: await update.message.reply_text(MESSAGES[lang]["not_authorized_admin"]); return
    db.set_user_state(cid, {"state": "waiting_for_broadcast_photo"}); await update.message.reply_text(MESSAGES[lang]["broadcast_photo_usage"])

async def broadcast_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid, lang = update.effective_chat.id, db.get_user_setting(update.effective_chat.id, 'language', 'ar')
    if cid not in ADMIN_CHAT_IDS: await update.message.reply_text(MESSAGES[lang]["not_authorized_admin"]); return
    db.set_user_state(cid, {"state": "waiting_for_broadcast_video"}); await update.message.reply_text(MESSAGES[lang]["broadcast_video_usage"])

async def on_startup(app: ApplicationBuilder):
    general_commands = [BotCommand("start", MESSAGES["en"]["command_start_desc"]), BotCommand("lang", MESSAGES["en"]["command_lang_desc"]), BotCommand("help", MESSAGES["en"]["command_help_desc"])]
    await app.bot.set_my_commands(general_commands, scope=BotCommandScopeDefault())
    admin_commands = general_commands + [BotCommand("stats", MESSAGES["ar"]["command_stats_desc"]), BotCommand("broadcast_text", MESSAGES["ar"]["command_broadcast_text_desc"]), BotCommand("broadcast_photo", MESSAGES["ar"]["command_broadcast_photo_desc"]), BotCommand("broadcast_video", MESSAGES["ar"]["command_broadcast_video_desc"])]
    for admin_id in ADMIN_CHAT_IDS:
        try:
            await app.bot.set_my_commands(admin_commands, scope=BotCommandScopeChat(chat_id=admin_id))
            logger.info(f"Set admin commands for chat ID: {admin_id}")
        except Exception as e: logger.error(f"Failed to set admin commands for {admin_id}: {e}")

def main():
    logger.info("Initializing database...")
    db.initialize_database()
    logger.info("Database initialization complete.")
    app = (ApplicationBuilder().token(TELEGRAM_TOKEN).arbitrary_callback_data(True).post_init(on_startup).build())
    app.add_handler(CommandHandler("start", start)); app.add_handler(CommandHandler("lang", lang_cmd)); app.add_handler(CommandHandler("help", help_cmd)); app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("broadcast_text", broadcast_text)); app.add_handler(CommandHandler("broadcast_photo", broadcast_photo)); app.add_handler(CommandHandler("broadcast_video", broadcast_video))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message)); app.add_handler(MessageHandler((filters.PHOTO | filters.VIDEO) & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(on_lang_button, pattern="^lang:"))
    app.add_handler(CallbackQueryHandler(show_financial_report, pattern="^show_report:"))
    app.add_handler(CallbackQueryHandler(calculate_purification_callback, pattern="^calc_purify:"))
    app.add_handler(CallbackQueryHandler(handle_profit_type_selection, pattern="^profit_type:"))
    logger.info("Bot is running...")
    app.run_polling()
    if db.conn: db.conn.close(); logger.info("Database connection closed.")

if __name__ == "__main__":
    main()