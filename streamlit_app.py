import streamlit as st
import os
import json
from pathlib import Path
import requests
import re
import time
import tempfile
from datetime import datetime

# ==============================
# Config
# ==============================

BASE_URL = "https://platform.reducto.ai"
REDUCTO_API_KEY = "18a01738a2899e13fec9306d77da9b376ec214e8033ebdecc2329ccc60f0dcba17fea0e7014e634ca2de0ec26fb3aa02"

# ==============================
# Helper Functions
# ==============================

def upload_file_to_reducto(fname: str, api_key: str) -> str:
    """
    Uploads a file to Reducto and returns the file_id (document URL).
    """
    file_path = Path(fname)

    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    # Step 1: Get presigned URL + file_id
    upload_resp = requests.post(
        f"{BASE_URL}/upload",
        headers={"Authorization": f"Bearer {api_key}"},
    )
    upload_resp.raise_for_status()
    upload_form = upload_resp.json()

    # Step 2: Upload file bytes to the presigned URL
    with file_path.open("rb") as f:
        put_resp = requests.put(upload_form["presigned_url"], data=f)
    put_resp.raise_for_status()

    return upload_form["file_id"]


def poll_job_until_complete(job_id, api_key, timeout=600, poll_interval=10, progress_bar=None, status_text=None):
    """
    Polls Reducto job status endpoint until job completes or timeout (seconds) exceeded.
    Returns the extraction result JSON once done.
    """
    headers = {"Authorization": f"Bearer {api_key}"}
    end_time = time.time() + timeout
    
    elapsed = 0

    while time.time() < end_time:
        resp = requests.get(f"{BASE_URL}/job/{job_id}", headers=headers)
        
        output = resp.json()
        result = output.get("result")
        
        if result:
            result = result.get("result")[0]

        resp.raise_for_status()
        status_resp = resp.json()

        job_status = status_resp.get("status")
        
        if status_text:
            status_text.text(f"Job status: {job_status} (elapsed: {elapsed}s)")
        
        if progress_bar and timeout > 0:
            progress = min(elapsed / timeout, 0.95)  # Cap at 95% until complete
            progress_bar.progress(progress)

        if job_status == "Completed":
            if progress_bar:
                progress_bar.progress(1.0)
            if status_text:
                status_text.text("Job completed successfully!")
            return result

        elif job_status in ["failed", "cancelled"]:
            raise RuntimeError(f"Job {job_id} ended with status: {job_status}")

        time.sleep(poll_interval)
        elapsed += poll_interval

    raise TimeoutError(f"Job {job_id} did not complete within {timeout} seconds.")


def post_process_amounts(data: dict) -> dict:
    """
    Post-process to ensure trailing minus signs are preserved.
    Handles various negative number formats.
    """
    def fix_trailing_minus(text: str) -> str:
        if not isinstance(text, str) or not text.strip():
            return text

        text = text.strip()

        trailing_minus_pattern = r'^([\d,]+\.?\d*)[\s]*(-|\−|–)[\s]*$'
        match = re.match(trailing_minus_pattern, text)
        if match:
            number = match.group(1)
            return f"{number}-"

        paren_pattern = r'^\(([\d,]+\.?\d*)\)$'
        match = re.match(paren_pattern, text)
        if match:
            number = match.group(1)
            return f"({number})"

        return text

    if isinstance(data, dict):
        return {k: post_process_amounts(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [post_process_amounts(item) for item in data]
    elif isinstance(data, str):
        return fix_trailing_minus(data)
    else:
        return data


def parse_amount_to_float(amount_str: str) -> float:
    """
    Parse an amount string to a float value.
    Handles: currency symbols ($, €, etc.), commas, trailing minus, parentheses, empty strings.
    """
    if not amount_str or not isinstance(amount_str, str):
        return 0.0
    
    amount_str = amount_str.strip()
    if not amount_str:
        return 0.0
    
    # Remove currency symbols
    amount_str = re.sub(r'[$€£₹¥]', '', amount_str)
    
    # Handle trailing minus (e.g., "1,234.56-")
    trailing_minus_pattern = r'^([\d,]+\.?\d*)[\s]*(-|\−|–)[\s]*$'
    match = re.match(trailing_minus_pattern, amount_str)
    if match:
        number_str = match.group(1)
        number_str = number_str.replace(',', '')
        return -float(number_str)
    
    # Handle parentheses (e.g., "(1,234.56)")
    paren_pattern = r'^\(([\d,]+\.?\d*)\)$'
    match = re.match(paren_pattern, amount_str)
    if match:
        number_str = match.group(1)
        number_str = number_str.replace(',', '')
        return -float(number_str)
    
    # Handle leading minus (e.g., "-1,234.56")
    if amount_str.startswith('-'):
        number_str = amount_str[1:].replace(',', '')
        return -float(number_str)
    
    # Regular positive number
    number_str = amount_str.replace(',', '')
    try:
        return float(number_str)
    except ValueError:
        return 0.0


def normalize_output_structure(data: dict) -> dict:
    """
    Normalize output structure to ensure all fields are always present,
    even if data is missing. Returns a consistent structure with default values.
    """
    # Handle None or non-dict input
    if not isinstance(data, dict):
        data = {}
    
    # Default structure with all fields
    normalized = {
        "company_name": data.get("company_name") if data.get("company_name") else None,
        "bank_name": data.get("bank_name") if data.get("bank_name") else None,
        "is_bank_statement": data.get("is_bank_statement") if "is_bank_statement" in data else False,
        "is_application_form": data.get("is_application_form") if "is_application_form" in data else False,
        "currency": data.get("currency") if data.get("currency") else None,
        "statement_period": data.get("statement_period") if data.get("statement_period") else None,
        "account_number": data.get("account_number") if data.get("account_number") else None,
        "transactions": data.get("transactions") if isinstance(data.get("transactions"), list) else [],
        "daily_ending_balance": data.get("daily_ending_balance") if isinstance(data.get("daily_ending_balance"), list) else [],
        "cheques": data.get("cheques") if isinstance(data.get("cheques"), list) else [],
        "fees": data.get("fees") if isinstance(data.get("fees"), list) else [],
        "starting_balance": data.get("starting_balance") if data.get("starting_balance") else None,
        "ending_balance": data.get("ending_balance") if data.get("ending_balance") else None,
        "nsf_data": data.get("nsf_data") if isinstance(data.get("nsf_data"), dict) else {
            "events": [],
            "summary": {
                "total_nsf_fees": 0,
                "unique_days_with_nsf": 0,
                "max_nsfs_in_any_7day_window": 0
            }
        }
    }
    
    # Ensure nsf_data has complete structure
    if not isinstance(normalized["nsf_data"], dict):
        normalized["nsf_data"] = {
            "events": [],
            "summary": {
                "total_nsf_fees": 0,
                "unique_days_with_nsf": 0,
                "max_nsfs_in_any_7day_window": 0
            }
        }
    else:
        # Ensure events array exists
        if "events" not in normalized["nsf_data"]:
            normalized["nsf_data"]["events"] = []
        
        # Ensure summary object exists with all fields
        if "summary" not in normalized["nsf_data"]:
            normalized["nsf_data"]["summary"] = {
                "total_nsf_fees": 0,
                "unique_days_with_nsf": 0,
                "max_nsfs_in_any_7day_window": 0
            }
        else:
            summary = normalized["nsf_data"]["summary"]
            if "total_nsf_fees" not in summary:
                summary["total_nsf_fees"] = 0
            if "unique_days_with_nsf" not in summary:
                summary["unique_days_with_nsf"] = 0
            if "max_nsfs_in_any_7day_window" not in summary:
                summary["max_nsfs_in_any_7day_window"] = 0
    
    return normalized


def calculate_running_balances(processed_data: dict) -> list:
    """
    Calculate running balances from starting_balance and transactions.
    Returns a list of dictionaries with date and calculated balance.
    """
    # Extract starting balance
    starting_balance_str = processed_data.get("starting_balance", "0")
    current_balance = parse_amount_to_float(starting_balance_str)
    
    # Extract transactions
    transactions = processed_data.get("transactions", [])
    
    # List to store calculated balances
    balance_records = []
    
    # Iterate through transactions
    for transaction in transactions:
        date = transaction.get("date", "")
        credit_str = transaction.get("credit", "")
        debit_str = transaction.get("debit", "")
        
        # Process credit (add to balance)
        if credit_str and credit_str.strip():
            credit_amount = parse_amount_to_float(credit_str)
            current_balance += credit_amount
        
        # Process debit (subtract from balance)
        if debit_str and debit_str.strip():
            debit_amount = parse_amount_to_float(debit_str)
            current_balance -= debit_amount
        
        # Store the balance for this transaction
        balance_records.append({
            "date": date,
            "balance": current_balance
        })
    
    return balance_records


def extract_with_enhanced_ocr_async(fname: str, system_prompt: str, schema: dict, api_key: str, progress_bar=None, status_text=None):
    """
    Enhanced extraction using asynchronous API call to handle large files without timeout.
    """
    # 1) Upload the file
    if status_text:
        status_text.text("Uploading file to Reducto...")
    document_url = upload_file_to_reducto(fname, api_key)

    payload = {
        "input": document_url,
        "parsing": {
            "retrieval": {
                "chunking": {
                    "chunk_mode": "page"
                },
            },
        },
        "instructions": {
            "schema": schema,
            "system_prompt": system_prompt,
        },
        "settings": {
            "ocr_system": "standard",
            "include_images": False,
            "optimize_for_latency": False,
            "array_extract": True,
        },
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    # 3) Submit async extraction job
    if status_text:
        status_text.text("Submitting extraction job...")
    resp = requests.post(f"{BASE_URL}/extract", json=payload, headers=headers)
    resp.raise_for_status()
    resp_json = resp.json()

    job_id = resp_json.get("job_id")
    if not job_id:
        raise RuntimeError("Job ID missing from async extract response")

    # 4) Poll job status until complete
    if status_text:
        status_text.text(f"Processing job {job_id}...")
    result_json = poll_job_until_complete(job_id=job_id, api_key=api_key, progress_bar=progress_bar, status_text=status_text)

    return result_json


# System prompt and schema (from original code)
system_prompt = """
CRITICAL FORMATTING RULES - READ CAREFULLY:

1. Extract ALL numeric values EXACTLY as they appear in the document
2. DO NOT modify or reformat any numbers
3. For negative amounts, preserve the EXACT format:
   - If minus appears AFTER the number (e.g., "1,234.56-"), keep it there
   - If minus appears BEFORE the number (e.g., "-1,234.56"), keep it there
   - If number is in parentheses (e.g., "(1,234.56)"), preserve the parentheses
4. Preserve ALL formatting: commas, decimal points, spaces, minus signs
5. Return ALL amounts as STRING values, not numbers
6. Do NOT perform any mathematical conversions or formatting changes
7. Extract the raw text EXACTLY as shown in the PDF

Examples of what to preserve:
- "1,234.56-" stays as "1,234.56-"
- "(1,234.56)" stays as "(1,234.56)"
- "-1,234.56" stays as "-1,234.56"

DOCUMENT TYPE CLASSIFICATION - BANK STATEMENT IDENTIFICATION:

A. Bank-Statement Indicators (Score +1 for each; need ≥3 to pass):
- Keywords: "Bank Statement", "Account Statement", "Statement of Account"
- Bank identifier: bank name/logo (e.g., "Wells Fargo Bank", "HDFC Bank Ltd.")
- Account details: masked/full account number (e.g., "Account #: ××1234")
- Statement period: date range (e.g., "01 Apr 2025 – 30 Apr 2025")
- Running-balance table with columns: Date | Description | Debit | Credit | Balance
- Opening/Closing balance summary (e.g., "Beginning Balance", "Ending Balance")

A2. MTD / Interim Bank-Statement Indicators (use when headers are missing or sparse; Score +1 each):
- Keywords suggesting partial period: "MTD", "Month to Date", "Interim Statement", "As of <date>", "Transactions for <Month YYYY>"
- Transaction table begins immediately (often on page 1) with columns like: Date | Description | Amount | (Debit/Credit) | Balance (balance may be missing)
- Date column shows many rows from a single month (or a tight recent window) in chronological order
- Bank-origin signals inside rows: ACH/CCD/PPD/NEFT/IMPS/UPI/RTGS/SWIFT/BIC/SEPA/CHECK/Cheque No./Routing/IFSC/Sort Code/IBAN
- Banking descriptors in lines: "ATM", "POS", "Mobile Deposit", "Online Transfer", "Overdraft/OD", "NSF/Returned Item", "Available Balance"
- Page footer/header on any page shows bank name/logo, branch/address, routing/sort/IFSC, or "Customer Service" contacts

A3. Pass Criteria (two safe paths; choose highest confidence):
- Full Statement Path: If Section A score ≥3 AND no exclusions in B → classify as BANK_STATEMENT (set is_bank_statement = TRUE)
- MTD Path: If ≥3 indicators from A2 AND at least 1 bank-origin signal (A2 bullets 4, 5, or 6) AND no exclusions in B → classify as BANK_STATEMENT (set is_bank_statement = TRUE)

B. Hard Exclusion Clues (Any one → immediate reject → NOT_BANK_STATEMENT):
- Invoice, receipt, payslip, tax form, insurance policy, loan agreement, term sheet, KYC form, marketing brochure
- Phrases: "Invoice #", "Purchase Order", "Salary Slip", "Policy Number", "Form 16", "Prospectus", "Memorandum"
- ERP/accounting/general ledger exports without bank-origin signals (e.g., columns like "GL Account", "Vendor ID", "Cost Center", "PO #", "Bill #") even if a "Date/Amount" table exists
- Only narrative text with no transaction list

C. Ambiguity Handling:
- If Full Statement Path score = 2 exactly → prefer false (lower risk of false positives) → set is_bank_statement = FALSE
- For MTD Path, require the bank-origin signal rule strictly (A2-4/5/6). If exactly 2 A2 indicators or none of (4/5/6) → prefer NOT_BANK_STATEMENT → set is_bank_statement = FALSE

DOCUMENT TYPE CLASSIFICATION - APPLICATION FORM IDENTIFICATION:

Loan-application forms typically contain (one or more of):
- Optional headings such as "Loan Application" – but note that some templates omit headings entirely
- Fixed-field labels: "Borrower", "Co-Borrower", "Business Name", "Legal Entity", "Owner(s) / Principal(s)", "Loan Amount", "Property Address", "Broker/Originator", "Interest Rate", "Signature", etc.
- Structured input areas like check-boxes or multi-column tables for Employment, Assets & Liabilities, Declarations, Business Financials, etc.
- Date fields in "MM/DD/YYYY" format beside labels such as "Date of Application" or "Date Prepared"
- Form fields, application sections, signature areas, checkboxes, input fields

Bank-statement first pages typically contain (NOT application forms):
- A bank's name or logo followed by a masked account number ("****1234")
- "Statement period" or "For the period DD MMM YYYY to DD MMM YYYY"
- Balance summary fields: "Opening Balance", "Closing Balance", "Deposits", "Withdrawals"
- Postal address of the account holder and disclaimer paragraphs ("Please review your statement carefully…", regulatory footers, etc.)

Set is_application_form = TRUE if the document is a loan application form (contains the application form characteristics above)
Set is_application_form = FALSE if it's a bank statement or other document type
If you are < 80% confident, return FALSE as a safe default

CURRENCY EXTRACTION:
- Extract the currency code from the document (look for currency symbols like $, €, £, ₹, etc. or explicit currency codes)
- Use ISO 4217 currency codes: USD for US Dollar, EUR for Euro, GBP for British Pound, INR for Indian Rupee, CAD for Canadian Dollar, AUD for Australian Dollar, etc.
- If currency is not explicitly stated, infer from currency symbols or context (e.g., $ typically means USD, € means EUR, £ means GBP, ₹ means INR)
- Return the 3-letter ISO currency code from the provided enum list

ACCOUNT INFORMATION EXTRACTION:
- Extract the company_name (account holder name or company name) from the statement header
- Extract the bank_name (financial institution name) from the statement header or logo area
- These are typically found at the top of the statement

TRANSACTION TABLE EXTRACTION - CRITICAL COMPLETENESS REQUIREMENTS:

MANDATORY: Extract ALL transactions from the transaction table. Missing even a single transaction is unacceptable.

Identification of Transaction Table:
- Look for tables with headers containing: "Date", "Description", "Transaction Description", "Details", "Debit", "Credit", "Withdrawal", "Deposit", "Amount", "Balance", "Running Balance", "Available Balance" 
- The transaction table is the MAIN table showing individual transaction entries with dates, descriptions, and amounts
- This table typically spans multiple pages - you MUST check ALL pages of the document
- Transaction tables may have different column layouts:
  * Format 1: Date | Description | Debit | Credit | Balance
  * Format 2: Date | Description | Amount | Balance (where Amount can be positive for credits, negative for debits)
  * Format 3: Date | Description | Withdrawal | Deposit | Balance
  * Format 4: Date | Description | Amount | Balance (with separate indication of debit/credit)
- Some statements may have transactions split across multiple tables or sections - extract from ALL of them

CRITICAL: SEPARATE DEPOSITS AND WITHDRAWALS SECTIONS:
- Many bank statements organize transactions into SEPARATE sections with clear headings:
  * "DEPOSITS" or "CREDITS" section (showing money coming in)
  * "WITHDRAWALS" or "DEBITS" section (showing money going out)
  * "CHECKS" or "CHECQUES" section (showing check transactions)
  * "ELECTRONIC TRANSFERS" or "ONLINE TRANSACTIONS" section
  * "ATM TRANSACTIONS" section
  * "FEE TRANSACTIONS" section
- When you see separate sections, you MUST extract transactions from ALL sections
- DO NOT extract only from the Deposits/Credits section - you MUST also extract from Withdrawals/Debits section
- DO NOT extract only from the Withdrawals/Debits section - you MUST also extract from Deposits/Credits section

Completeness Requirements:
1. Extract EVERY SINGLE ROW from the transaction table - do not skip any transactions
2. Check ALL pages of the document - transactions may continue across multiple pages
3. Extract transactions from ALL sections if transactions are split into separate sections (Deposits, Withdrawals, Credits, Debits, Checks, etc.) - this is CRITICAL
4. Include transactions that appear in summary sections if they are individual transaction entries
5. Do not exclude any transaction based on amount, type, or description

Required Fields for Each Transaction (ALL fields are REQUIRED):
- date: Extract the transaction date exactly as shown (preserve format: MM/DD/YYYY, DD-MMM-YYYY, etc.)
- description: Extract the FULL transaction description/description text exactly as shown in the document
- debit: Extract debit amount as EXACT string (if no debit, use empty string "")
- credit: Extract credit amount as EXACT string (if no credit, use empty string "")
- balance: Extract the running balance as EXACT string with all formatting preserved

DAILY ENDING BALANCE TABLE EXTRACTION:
- Look for a table titled "BALANCE BY DATE" or similar heading
- This table has DATE and BALANCE columns
- Extract ALL date-balance pairs from this table

NSF (NON-SUFFICIENT FUNDS) EXTRACTION:
- Include: ACH returns for insufficient/uncollected funds (NACHA codes R01, R09), cheque/check returns for insufficient/uncollected funds, and their associated fees
- Exclude: Overdraft fees/interest/transfers, fee reversals, stop-payment fees, documentation issues
"""

schema = {
    "type": "object",
    "properties": {
        "company_name": {
            "type": ["string", "null"],
            "description": "Company name or account holder name from the bank statement. Return null if not found."
        },
        "bank_name": {
            "type": ["string", "null"],
            "description": "Bank name or financial institution name from the bank statement. Return null if not found."
        },
        "is_bank_statement": {
            "type": "boolean",
            "description": "TRUE if the document is a bank statement, FALSE otherwise. Always return a boolean value."
        },
        "is_application_form": {
            "type": "boolean",
            "description": "TRUE if the document is an application form, FALSE otherwise. Always return a boolean value."
        },
        "currency": {
            "anyOf": [
                {
                    "type": "string",
                    "enum": [
                        "USD", "EUR", "GBP", "INR", "CAD", "AUD", "JPY", "CHF", "CNY", "SGD",
                        "HKD", "NZD", "KRW", "MXN", "BRL", "RUB", "ZAR", "SEK", "NOK", "DKK",
                        "PLN", "CZK", "HUF", "RON", "BGN", "HRK", "TRY", "THB", "MYR", "IDR",
                        "PHP", "VND", "BDT", "PKR", "EGP", "NGN", "KES", "GHS", "UGX", "TZS"
                    ]
                },
                {"type": "null"}
            ],
            "description": "The ISO 4217 currency code (e.g., USD, EUR, GBP, INR, CAD, AUD, etc.). Return null if not found."
        },
        "statement_period": {
            "type": ["string", "null"],
            "description": "Statement date range. Return null if not found."
        },
        "account_number": {
            "type": ["string", "null"],
            "description": "Account number from headers. Return null if not found."
        },
        "transactions": {
            "type": "array",
            "description": "All transaction line items from the bank statement. Extract from ALL pages and ALL sections.",
            "items": {
                "type": "object",
                "properties": {
                    "date": {
                        "type": "string",
                        "description": "Transaction date - extract exactly as shown in the document"
                    },
                    "description": {
                        "type": "string",
                        "description": "Full transaction description text exactly as shown in the document"
                    },
                    "debit": {
                        "type": "string",
                        "description": "Debit amount (withdrawals/money out) as EXACT string. Use empty string \"\" if this is a credit/deposit transaction."
                    },
                    "credit": {
                        "type": "string",
                        "description": "Credit amount (deposits/money in) as EXACT string. Use empty string \"\" if this is a debit/withdrawal transaction."
                    },
                    "balance": {
                        "type": "string",
                        "description": "Running balance as EXACT string with all formatting preserved"
                    }
                },
                "required": ["date","description","debit","credit","balance"]
            }
        },
        "daily_ending_balance": {
            "type": "array",
            "description": "Daily ending balance table - separate from transaction table.",
            "items": {
                "type": "object",
                "properties": {
                    "date": {
                        "type": "string",
                        "description": "Date in MM/DD format"
                    },
                    "ending_balance": {
                        "type": "string",
                        "description": "Daily ending balance as EXACT string"
                    }
                },
                "required": ["date", "ending_balance"]
            }
        },
        "cheques": {
            "type": "array",
            "description": "Cheques table - a separate table listing cheque information.",
            "items": {
                "type": "object",
                "properties": {
                    "cheque_number": {
                        "type": "string",
                        "description": "Cheque number"
                    },
                    "date": {
                        "type": "string",
                        "description": "Cheque date"
                    },
                    "amount": {
                        "type": "string",
                        "description": "Cheque amount as EXACT string"
                    },
                    "status": {
                        "type": "string",
                        "description": "Cheque status"
                    },
                    "description": {
                        "type": "string",
                        "description": "Cheque description or payee information"
                    },
                    "is_included_in_transaction_table": {
                        "type": "boolean",
                        "description": "TRUE if this cheque entry also appears in the main transactions table"
                    }
                },
                "required": ["cheque_number", "date", "amount", "is_included_in_transaction_table"]
            }
        },
        "fees": {
            "type": "array",
            "description": "Fees table - ONLY extract fees if there is an EXPLICIT fees table in the document.",
            "items": {
                "type": "object",
                "properties": {
                    "date": {
                        "type": "string",
                        "description": "Fee transaction date"
                    },
                    "description": {
                        "type": "string",
                        "description": "Fee description as EXACT string"
                    },
                    "amount": {
                        "type": "string",
                        "description": "Fee amount as EXACT string"
                    },
                    "fee_type": {
                        "type": "string",
                        "description": "Type of fee"
                    },
                    "balance": {
                        "type": "string",
                        "description": "Balance after fee transaction"
                    },
                    "is_included_in_transaction_table": {
                        "type": "boolean",
                        "description": "TRUE if this fee entry also appears in the main transactions table"
                    }
                },
                "required": ["date", "description", "amount", "is_included_in_transaction_table"]
            }
        },
        "starting_balance": {
            "type": ["string", "null"],
            "description": "Starting balance as exact string. Return null if not found."
        },
        "ending_balance": {
            "type": ["string", "null"],
            "description": "Ending balance as exact string. Return null if not found."
        },
        "nsf_data": {
            "type": "object",
            "description": "NSF (Non-Sufficient Funds) events.",
            "properties": {
                "events": {
                    "type": "array",
                    "description": "Array of NSF events",
                    "items": {
                        "type": "object",
                        "properties": {
                            "ach_return_code": {
                                "anyOf": [
                                    {"type": "string", "enum": ["R01", "R09"]},
                                    {"type": "null"}
                                ],
                                "description": "ACH return code"
                            },
                            "date_posted": {
                                "type": ["string", "null"],
                                "description": "Date when the NSF event was posted"
                            },
                            "counterparty_name": {
                                "type": ["string", "null"],
                                "description": "Counterparty name"
                            },
                            "original_attempt_amount": {
                                "type": ["number", "null"],
                                "description": "Original amount that was attempted"
                            },
                            "fee_amount": {
                                "type": ["number", "null"],
                                "description": "NSF fee amount"
                            },
                            "description_raw": {
                                "type": "string",
                                "description": "Exact source text"
                            },
                            "confidence": {
                                "type": "number",
                                "description": "Confidence score 0.0-1.0"
                            }
                        },
                        "required": ["ach_return_code", "date_posted", "counterparty_name", "original_attempt_amount", "fee_amount", "description_raw", "confidence"]
                    }
                },
                "summary": {
                    "type": "object",
                    "description": "Summary statistics",
                    "properties": {
                        "total_nsf_fees": {
                            "type": "number",
                            "description": "Sum of all fee amounts"
                        },
                        "unique_days_with_nsf": {
                            "type": "integer",
                            "description": "Count of distinct dates"
                        },
                        "max_nsfs_in_any_7day_window": {
                            "type": "integer",
                            "description": "Max NSF events in any 7-day window"
                        }
                    },
                    "required": ["total_nsf_fees", "unique_days_with_nsf", "max_nsfs_in_any_7day_window"]
                }
            },
            "required": ["events", "summary"]
        }
    },
    "required": [
        "company_name",
        "bank_name",
        "is_bank_statement",
        "is_application_form",
        "currency",
        "statement_period",
        "account_number",
        "transactions",
        "daily_ending_balance",
        "cheques",
        "fees",
        "starting_balance",
        "ending_balance",
        "nsf_data"
    ]
}


# ==============================
# Streamlit UI
# ==============================

st.set_page_config(page_title="New-pipeline MCA", page_icon="🏦", layout="wide")

st.title("New-pipeline MCA")
st.markdown("Upload PDF bank statements to extract structured data using Reducto AI")

# Sidebar
with st.sidebar:
    st.header("About")
    st.markdown("""
    This app extracts structured data from bank statement PDFs including:
    - Account information
    - Transactions
    - Daily balances
    - Cheques
    - Fees
    - NSF data
    """)
    st.markdown("---")
    st.success("✓ API Key configured")

# Main content
# File uploader
uploaded_files = st.file_uploader(
    "Upload Bank Statement PDF(s)",
    type=["pdf"],
    accept_multiple_files=True,
    help="Upload one or more PDF files"
)

if uploaded_files:
    st.success(f"✓ {len(uploaded_files)} file(s) uploaded")
    
    # Process button
    if st.button("🚀 Extract Data", type="primary"):
        # Process each file
        for idx, uploaded_file in enumerate(uploaded_files, 1):
            st.markdown("---")
            st.subheader(f"Processing {idx}/{len(uploaded_files)}: {uploaded_file.name}")
            
            # Create a temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
                tmp_file.write(uploaded_file.read())
                tmp_file_path = tmp_file.name
            
            try:
                # Create progress indicators
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                # Extract data
                extracted_data = extract_with_enhanced_ocr_async(
                    fname=tmp_file_path,
                    system_prompt=system_prompt,
                    schema=schema,
                    api_key=REDUCTO_API_KEY,
                    progress_bar=progress_bar,
                    status_text=status_text
                )
                
                # Normalize and process
                normalized_data = normalize_output_structure(extracted_data)
                processed_data = post_process_amounts(normalized_data)
                
                # Calculate balances
                balance_records = calculate_running_balances(processed_data)
                
                # Get filename stem for downloads
                pdf_stem = Path(uploaded_file.name).stem
                
                st.success(f"✓ Successfully processed: {uploaded_file.name}")
                
                # Display results in tabs
                tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
                    "📊 Summary", 
                    "💳 Transactions", 
                    "📝 Cheques", 
                    "⚠️ NSF Events",
                    "💰 Fees",
                    "📄 Raw JSON"
                ])
                
                with tab1:
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown("**Account Information**")
                        st.write(f"**Company:** {processed_data.get('company_name', 'N/A')}")
                        st.write(f"**Bank:** {processed_data.get('bank_name', 'N/A')}")
                        st.write(f"**Account #:** {processed_data.get('account_number', 'N/A')}")
                        st.write(f"**Period:** {processed_data.get('statement_period', 'N/A')}")
                        st.write(f"**Currency:** {processed_data.get('currency', 'N/A')}")
                    
                    with col2:
                        st.markdown("**Balances**")
                        st.write(f"**Starting:** {processed_data.get('starting_balance', 'N/A')}")
                        st.write(f"**Ending:** {processed_data.get('ending_balance', 'N/A')}")
                        st.write(f"**Is Bank Statement:** {processed_data.get('is_bank_statement', False)}")
                    
                    # Metrics row
                    st.markdown("---")
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Transactions", len(processed_data.get('transactions', [])))
                    with col2:
                        # Filter cheques without description
                        cheques_without_desc = [c for c in processed_data.get('cheques', []) 
                                               if not c.get('description') or c.get('description').strip() == '']
                        st.metric("Cheques", len(cheques_without_desc))
                    with col3:
                        st.metric("NSF Events", len(processed_data.get('nsf_data', {}).get('events', [])))
                    with col4:
                        st.metric("Fees", len(processed_data.get('fees', [])))
                
                with tab2:
                    st.markdown("### 💳 Transactions")
                    transactions = processed_data.get('transactions', [])
                    if transactions:
                        st.dataframe(transactions, use_container_width=True, height=500)
                        st.metric("Total Transactions", len(transactions))
                    else:
                        st.info("No transactions found")
                
                with tab3:
                    st.markdown("### 📝 Cheques")
                    cheques = processed_data.get('cheques', [])
                    # Filter out cheques that have a description
                    cheques_filtered = [c for c in cheques 
                                       if not c.get('description') or c.get('description').strip() == '']
                    if cheques_filtered:
                        st.dataframe(cheques_filtered, use_container_width=True, height=500)
                        st.metric("Total Cheques (without description)", len(cheques_filtered))
                    else:
                        st.info("No cheques found (excluding cheques with descriptions)")
                
                with tab4:
                    st.markdown("### ⚠️ NSF (Non-Sufficient Funds) Events")
                    nsf_data = processed_data.get('nsf_data', {})
                    nsf_events = nsf_data.get('events', [])
                    nsf_summary = nsf_data.get('summary', {})
                    
                    # NSF Summary
                    st.markdown("**Summary**")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total NSF Fees", f"${nsf_summary.get('total_nsf_fees', 0):.2f}")
                    with col2:
                        st.metric("Unique Days with NSF", nsf_summary.get('unique_days_with_nsf', 0))
                    with col3:
                        st.metric("Max NSFs in 7-day Window", nsf_summary.get('max_nsfs_in_any_7day_window', 0))
                    
                    st.markdown("---")
                    
                    # NSF Events
                    if nsf_events:
                        st.markdown("**NSF Events**")
                        st.dataframe(nsf_events, use_container_width=True, height=400)
                        st.metric("Total NSF Events", len(nsf_events))
                    else:
                        st.info("No NSF events found")
                
                with tab5:
                    st.markdown("### 💰 Fees")
                    fees = processed_data.get('fees', [])
                    if fees:
                        st.dataframe(fees, use_container_width=True, height=500)
                        st.metric("Total Fees", len(fees))
                    else:
                        st.info("No fees found")
                
                with tab6:
                    st.markdown("### 📄 Raw JSON Data")
                    st.json(processed_data)
                
                # Download buttons
                st.markdown("---")
                col1, col2 = st.columns(2)
                with col1:
                    st.download_button(
                        label="📥 Download Processed JSON",
                        data=json.dumps(processed_data, indent=4, ensure_ascii=False),
                        file_name=f"{pdf_stem}_processed.json",
                        mime="application/json"
                    )
                with col2:
                    st.download_button(
                        label="📥 Download Balances JSON",
                        data=json.dumps(balance_records, indent=4, ensure_ascii=False),
                        file_name=f"{pdf_stem}_balances.json",
                        mime="application/json"
                    )
                
            except Exception as e:
                st.error(f"❌ Error processing {uploaded_file.name}: {str(e)}")
                with st.expander("See error details"):
                    st.exception(e)
            
            finally:
                # Clean up temp file
                try:
                    os.unlink(tmp_file_path)
                except:
                    pass
        
        st.markdown("---")
        st.success("🎉 All files processed successfully!")

