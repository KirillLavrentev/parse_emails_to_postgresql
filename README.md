# parse_emails_to_postgresql
The program reads emails via IMAP, parse them and save necessary info to PostgreSQL.

The program connects to the specified mailbox via IMAP, reads messages and parses html-format letters in utf-8, koi8-r, windows-1251 encodings. Then the saved emails are uploaded to PostgreSQL.
