import airflow
from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

import json
import subprocess
import imaplib
import email
import psycopg2
from html.parser import HTMLParser
import re


# -----------------------------------IMPORTS SECTION---------------------------------------------------------

default_args = {
    'owner': '<YOUR NAME>',
    'depends_on_past': False,
    'start_date': datetime(2019, 2, 8, 0, 0, 0),
    'execute_timeout': timedelta(hours=18),
    'retries': 1,
}

dag = DAG(
    dag_id='parse_mails_to_Postgres',
    catchup=False,
    default_args=default_args,
    description='Save mails from the <email> to Postgres',
    schedule_interval="*/1 * * * *",
    max_active_runs=1,
)


# Парсер html сообщений
class MyParser(HTMLParser):

    def __init__(self):
        HTMLParser.__init__(self)
        self._in_topic_td = False
        self._in_topic_anchor = False
        self._topic_list = []
        self._current_topic = {}

    def handle_data(self, data):
        if ((len(data)) > 2 and (re.search("[\\n\\r<!\-\-]*",data).group(0)=='')):  
            self._current_topic['title'] = data
            self._topic_list.append(self._current_topic)
            self._current_topic = {}
            
    def feed(self, data):
        try:
            HTMLParser.feed(self, data)
            return self._topic_list
        except Exception as e:
            log(unicode(traceback.format_exc()))
            print('ERROR:', e)
            return None

def rus_date_to_eng(date):
    dicti = {'января':'January','февраля':'February','марта':'March','апреля':'April','мая':'May','июня':'June',
            'июля':'July','августа':'August','сентября':'September','ноября':'November','декабря':'December'}
    date = date.strip()
    try:
        day = date.split('(UTC')[0].split(' ')[0]
        month = dicti[date.split('(UTC')[0].split(' ')[1]]
        year = date.split('(UTC')[0].split(' ')[2]
        time = date.split('(UTC')[0].split(' ')[4]
        eng_date = month + ' ' + day +', ' + year + ' ' + time
        return(eng_date)
    except:
        return(date)

def correct_code_checker(text):
    rus_upper = ['А','Б','В','Г','Д','Е','Ё','Ж','З','И','Й','К','Л','М','Н','О','П',
                 'Р','С','Т','У','Ф','Х','Ц','Ч','Ш','Щ','Ъ','Ы','Ь','Э','Ю','Я']
    rus_lower = ['а','б','в','г','д','е','ё','ж','з','и','й','к','л','м','н','о','п',
                 'р','с','т','у','ф','х','ц','ч','ш','щ','ъ','ы','ь','э','ю','я']
    strange_letters = ['╟','┌','╫','╦','╨','╬','╡', '▓','╦','╬','░','╩','╣','│','╢','╬','┤','·','┌','©','▐','÷','≈',
                      'џ','¶','°','»','С','ѓ','Ѓ','µ','Ђ','„','‰','Ћ','Ў','±','”','і','ґ','Ќ','ї','Џ',
                       '‡','€','є','‹','…','ћ','Њ','†','љ','ќ']

    up_count = 0
    low_count = 0
    other_letters = 0

    for i in text:
        if i in rus_upper:
            up_count+=1
        elif i in rus_lower:
            low_count+=1
        elif i in strange_letters:
            other_letters+=1

    try:
        if (up_count/low_count) > 0.65 or (up_count == 0) or (other_letters > 8):
            return False # кодировка неправильная
        else:
            return True  # кодировка правильная
    except ZeroDivisionError:
        return False

def find_atributes(body):
    # определяем язык атрибутов
    try:
        rus = re.search("(От:)|(Отправлено:)|(Кому:)|(Тема:)",body).group(0)
    except AttributeError as e:
        rus = None
    if(rus):
        email_from = body[body.find('От:') + len('От:'):body.find('Отправлено:')]
        if (body.find('Кому:')!=-1):
            sent_date = rus_date_to_eng(body[body.find('Отправлено:')+len("Отправлено:"):body.find('Кому:')])
            email_to = body[body.find('Кому:')+len('Кому:'):body.find('Тема:')]
            subject = body[body.find('Тема:') + len('Тема:'):body.find('\n',body.find('Тема:'))]
            body1 = body[body.find('\n',body.find('Тема:')):]
        else:
            email_to = 'model@company.ru'
            sent_date = rus_date_to_eng(body[body.find('Отправлено:') + len("Отправлено:"):body.find('Тема:')])
            subject = body[body.find('Тема:') + len('Тема:'):body.find('\n',body.find('Тема:'))]
            body1 = body[body.find('\n',body.find('Тема:')):] 
        if (email_from == '') or (sent_date == ''):
            return None
        else:
            return(email_from, sent_date, email_to, subject, body1)
        
    else:
        email_from = body[body.find('From:') + len('From:'):body.find('Sent:')]
        if (body.find('To:')!=-1):
            sent_date = body[body.find('Sent:')+len('Sent:'):body.find('To:')]
            email_to = body[body.find('To:')+len('To:'):body.find('Subject:')]
            subject = body[body.find('Subject:')+len('Subject:'):body.find('\n',body.find('Subject:'))]
            body1 = body[body.find('\n',body.find('Subject:')):]
        else:
            email_to = 'model@company.ru'
            sent_date = body[body.find('Sent:')+len('Sent:'):body.find('Subject:')]
            subject = body[body.find('Subject:')+len('Subject:'):body.find('\n',body.find('Subject:'))]
            body1 = body[body.find('\n',body.find('Subject:')):]

        if (email_from == '') or (sent_date == ''):
            return None
        else:
            return(email_from, sent_date, email_to, subject, body1)

def main():
    cmd = ['/usr/bin/secret','https://org.company.ru:8200/v1/postgresql/secret/user','--json',
    'https://org.company.ru:8200/v1/postgresql/public/balancer','--json']

    jsonquery = json.loads((subprocess.Popen(cmd, stdout=subprocess.PIPE).communicate()[0]).decode('utf-8').replace('\n', '').replace("}{", ","))
    dbname = json.dumps(jsonquery['database']).strip('"')
    user_postgres = json.dumps(jsonquery['user']).strip('"')
    passw_postgres = json.dumps(jsonquery['password']).strip('"')
    host = json.dumps(jsonquery['socketAddress.native']).strip('"').split(":")[0]

    conn = psycopg2.connect(dbname = dbname, user = user_postgres, password = passw_postgres, host = host)
    cursor = conn.cursor()

    cmd = ['/usr/bin/secret','https://org.company.ru.ru:8200/v1/secret/user','--json']
    jsonquery = json.loads((subprocess.Popen(cmd, stdout=subprocess.PIPE).communicate()[0]).decode('utf-8').replace('\n', '').replace("}{", ","))
    mail_name = json.dumps(jsonquery['login']).strip('"')
    passw_mail = json.dumps(jsonquery['password']).strip('"')
    domain = 'imap.' + json.dumps(jsonquery['domainFqdn']).strip('"')

    print("Hello!")
    mail = imaplib.IMAP4_SSL(domain, 993)
    print(mail.welcome)
    mail.login(mail_name, passw_mail)
    mail.list()
    mail.select('INBOX')

    # Шапка письма
    msg = """
    From: {}
    Sent: {}
    To: {}
    Subject: {}
    =======================================================================================
    {}
    +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    """

    insert_query = 'INSERT INTO parse_emails ("from", sent, "to", subject, message) VALUES ({},TIMESTAMP {},{},{},{});'

    typ, data_del = mail.search(None, 'ALL') # для корректного удаления писем
    result, data = mail.uid('search', None, "All") # (ALL/UNSEEN)
    i = len(data[0].split())
    print('Всего найдено {} новых писем\n'.format(i))

    for x in range(i):
        latest_email_uid = data[0].split()[x]
        id_del = data_del[0].split()[x]
        result, email_data = mail.uid('fetch', latest_email_uid, '(RFC822)')
        # result, email_data = conn.store(num,'-FLAGS','\\Seen')
        # this might work to set flag to seen, if it doesn't already
        raw_email = email_data[0][1]
        raw_email_string = raw_email.decode('utf-8')
        email_message = email.message_from_string(raw_email_string)

            # Header Details
        date_tuple = email.utils.parsedate_tz(email_message['Date'])
        if date_tuple:
            local_date = datetime.fromtimestamp(email.utils.mktime_tz(date_tuple))
            sent_date = "%s" %(str(local_date.strftime("%a, %d %b %Y %H:%M:%S")))
        email_from = str(email.header.make_header(email.header.decode_header(email_message['From'])))
        email_to = str(email.header.make_header(email.header.decode_header(email_message['To'])))
        subject = str(email.header.make_header(email.header.decode_header(email_message['Subject'])))

        # Body details
        for part in email_message.walk():
            coder_counter = 0
            if part.get_content_type() == "text/plain":
                for decoder in ['utf-8','koi8-r','windows-1251']:
                    try:
                        body = part.get_payload(decode=True).decode(decoder)
                        if(find_atributes(body)):
                            email_from, sent_date, email_to, subject, body_decode = find_atributes(body)
                        else:
                            body_decode = body


                        if len(body_decode) < 20:
                            check_code = correct_code_checker(subject[-60:] + ' ' + body_decode[:20])
                        else:
                            check_code = correct_code_checker(body_decode[:100])

                        if check_code:
                            # print("Прошло проверку на кодировку ", decoder)
                            output = msg.format(email_from.replace('\'',' '), sent_date, email_to.replace('\'',' '), subject.replace('\'',' '), body_decode.replace('\'',' '))
                            try:
                                sd = datetime.strptime(sent_date, "%B %d, %Y %H:%M:%S")
                                cursor.execute(insert_query.format('\''+email_from.replace('\'',' ')+'\'', '\'' + sent_date + '\'','\'' + email_to.replace('\'',' ')+'\'', '\''+subject.replace('\'',' ')+'\'','\''+ output.replace('\'',' ')+'\''))
                                print('Выполнили запрос на сохранение text/plain в {}. Письмо номер: {}'.format(decoder, x))
                            except ValueError as e:
                                sent_date = 'now()'
                                cursor.execute(insert_query.format('\''+email_from.replace('\'',' ')+'\'', '\'' + sent_date + '\'','\'' + email_to.replace('\'',' ')+'\'', '\''+subject.replace('\'',' ')+'\'','\''+ output.replace('\'',' ')+'\''))
                                print('Выполнили запрос на сохранение text/plain в {}. Письмо номер: {}'.format(decoder, x))
                            coder_counter = 0
                            break
                        else:
                            # print('Письмо номер {} не прошло проверку на кодировку {}'.format(x, decoder))
                            coder_counter+=1
                        if (coder_counter == 3):
                            output = msg.format(email_from.replace('\'',' '), sent_date, email_to.replace('\'',' '), subject.replace('\'',' '), body_decode.replace('\'',' '))
                            output = "** НЕ РАСПОЗНАНА КОДИРОВКА **\n" + output
                            try:
                                sd = datetime.strptime(sent_date, "%B %d, %Y %H:%M:%S")
                                cursor.execute(insert_query.format('\''+email_from.replace('\'',' ')+'\'', '\'' + sent_date.replace('\'',' ') + '\'','\'' + email_to.replace('\'',' ')+'\'', '\''+subject.replace('\'',' ')+'\'','\''+ output.replace('\'',' ')+'\''))
                                print('Выполнили запрос на сохранение text/plain в {}. Письмо номер: {}'.format(decoder, x))
                            except ValueError as e:
                                sent_date = 'now()'
                                cursor.execute(insert_query.format('\''+email_from.replace('\'',' ')+'\'', '\'' + sent_date.replace('\'',' ') + '\'','\'' + email_to.replace('\'',' ')+'\'', '\''+subject.replace('\'',' ')+'\'','\''+ output.replace('\'',' ')+'\''))
                                print('Выполнили запрос на сохранение text/plain в {}. Письмо номер: {}'.format(decoder, x))
                            coder_counter = 0

                    except (UnicodeEncodeError, UnicodeDecodeError, ValueError) as error:
                        print('Не могу сохранить письмо типа text/plain в {}. Письмо номер: {}'.format(decoder, x))
                    
                    
            elif part.get_content_type() == "text/html":
                for decoder in ['koi8-r','windows-1251','utf-8']:
                    body = part.get_payload(decode=True)
                    try:
                        body_decode = body.decode(decoder)   
                        topic_parser = MyParser()
                        message = ''
                        for line in topic_parser.feed(body_decode):
                            message += line['title'] + ' '
                        if(find_atributes(message)):
                            email_from, sent_date, email_to, subject, body_decode = find_atributes(message)
                        else:
                            body_decode = message

                        if len(body_decode) < 20:
                            check_code = correct_code_checker(subject[-60:] + ' ' + body_decode[:20])
                        else:
                            check_code = correct_code_checker(body_decode[:100])
                        
                        if check_code:
                            # print('Прошло проверку на кодировку ', decoder)
                            output = msg.format(email_from.replace('\'',' '), sent_date, email_to.replace('\'',' '), subject.replace('\'',' '), body_decode.replace('\'',' '))
                            try:
                                sd = datetime.strptime(sent_date, "%B %d, %Y %H:%M:%S")
                                cursor.execute(insert_query.format('\''+email_from.replace('\'',' ')+'\'', '\'' + sent_date + '\'','\'' + email_to.replace('\'',' ')+'\'', '\''+subject.replace('\'',' ')+'\'','\''+ output.replace('\'',' ')+'\''))
                                print('Выполнили запрос на сохранение text/html в {}. Письмо номер: {}'.format(decoder, x))
                            except ValueError as e:
                                sent_date = 'now()'
                                cursor.execute(insert_query.format('\''+email_from.replace('\'',' ')+'\'', '\'' + sent_date + '\'','\'' + email_to.replace('\'',' ')+'\'', '\''+subject.replace('\'',' ')+'\'','\''+ output.replace('\'',' ')+'\''))
                                print('Выполнили запрос на сохранение text/html в {}. Письмо номер: {}'.format(decoder, x))
                            coder_counter = 0
                            break

                        else:
                            # print('Не прошло проверку на кодировку ',decoder)
                            coder_counter +=1

                        if (coder_counter == 3):
                            output = msg.format(email_from.replace('\'',' '), sent_date, email_to.replace('\'',' '), subject.replace('\'',' '), body_decode.replace('\'',' '))
                            output = "** НЕ РАСПОЗНАНА КОДИРОВКА **\n" + output
                            try:
                                sd = datetime.strptime(sent_date, "%B %d, %Y %H:%M:%S")
                                cursor.execute(insert_query.format('\''+email_from.replace('\'',' ')+'\'', '\'' + sent_date.replace('\'',' ') + '\'','\'' + email_to.replace('\'',' ')+'\'', '\''+subject.replace('\'',' ')+'\'','\''+ output.replace('\'',' ')+'\''))
                                print('Выполнили запрос на сохранение text/plain в {}. Письмо номер: {}'.format(decoder, x))
                            except ValueError as e:
                                sent_date = 'now()'
                                cursor.execute(insert_query.format('\''+email_from.replace('\'',' ')+'\'', '\'' + sent_date.replace('\'',' ') + '\'','\'' + email_to.replace('\'',' ')+'\'', '\''+subject.replace('\'',' ')+'\'','\''+ output.replace('\'',' ')+'\''))
                                print('Выполнили запрос на сохранение text/plain в {}. Письмо номер: {}'.format(decoder, x))
                            coder_counter = 0
                            
                    except (UnicodeEncodeError, UnicodeDecodeError, ValueError) as error:
                        print('\nПисьмо с порядковым номером {} не может быть сохранено в кодировке {}.\nОно имеет тип {}\n\n.'.format(x, decoder, part.get_content_type()))
                        print(error)

                
            else:
                #print('Письмо с порядковым номером {} не может быть прочитано.\n Оно имеет тип {}.\n\n'.format(x, part.get_content_type()))
                # with open('errors.txt','a') as f:
                #         f.write('\n\nПисьмо с порядковым номером {} не может быть прочитано.\n Оно имеет тип {}. \n\n'.format(x, part.get_content_type()))
                continue

        conn.commit()  # делаем комит изменений
        mail.store(id_del, '+FLAGS', '\\Deleted') # отмечаем прочитанное письмо флагом для удаления
        print('Прочитали письмо номер: ',x)

    conn.close() # закрываем соединение с PostgreSQL
    mail.expunge() # удаляем все прочитанные письма
    mail.close()   # выходим из ящика
    mail.logout()


SendMessages = PythonOperator(
    task_id='SendMessagesToPostgres',
   # provide_context=False,
    python_callable=main,
    queue='cluster',
    dag=dag,
    )