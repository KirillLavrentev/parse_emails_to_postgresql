# parse_emails_to_postgresql
The program connects to the specified mailbox via IMAP, reads all messages, parses html-format letters into utf-8, koi8-r, windows-1251 encodings, and saves necessary info to PostgreSQL. Necessary info is fields "From","Sent","To", "Subject", message body.

The program is designed as a DAG for AirFlow to run every minute (* * * * \*).
