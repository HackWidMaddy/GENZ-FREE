from flask import Flask, render_template, request, jsonify,send_file,redirect,send_file
import threading
import discord
import time
import ast
from io import BytesIO
import sqlite3
import requests
import json

with open('config.json') as temp:
    json_str = temp.read()
# Convert JSON string to dictionary
data_dict = json.loads(json_str)


CHUNK_SIZE = 25 * 1024 * 1024  # 25 MB in bytes
conn = sqlite3.connect('genzfree.db')
cursor = conn.cursor()

cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
tb = []
for table in tables:
    tb.append(table[0])
print(tb)

if len(tb) == 0:
    cursor.execute("""CREATE TABLE fileinfo(
            file_name text,
            file_url text,
            file_split text,
            message_id text,
            size text
           
                           )""")
    conn.commit()

else:
    if len(tb) == 1 or len(tb) == 2:
        pass
    else:
        cursor.execute("""CREATE TABLE fileinfo(
            file_name text,
            file_url text,
            file_split text,
            message_id text
        )""")
        conn.commit()

TOKEN = data_dict['TOKEN']
CHANNEL_ID = data_dict['CHANNEL_ID']  # Replace with your Discord channel ID

app = Flask(__name__)
client = discord.Client()


def run_flask():
    app.run(debug=True, use_reloader=False)

def run_discord():
    client.run(TOKEN)

@client.event
async def on_ready():
    print(f'We have logged in as {client.user}')


def retrieve_and_merge_file(file_urls):
    # Initialize an empty byte array to store the merged file content
    merged_content = bytearray()

    for url in file_urls:
        response = requests.get(url)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Append the content of the current chunk to the merged content
            merged_content.extend(response.content)
        else:
            print(f"Failed to retrieve chunk from {url}")

    # Create a BytesIO object from the merged content
    merged_file = BytesIO(merged_content)

    return merged_file

async def send_large_file(channel, file_content, file_name):
    # Create a new SQLite connection and cursor within the function
    urls = []
    conn = sqlite3.connect('genzfree.db')
    cursor = conn.cursor()
    file_size_mb = len(file_content) / (1024 * 1024)
        # print(f"File size: {file_size_mb:.2f} MB")
    updated_file_size = "{:.2f}".format(file_size_mb)
    # Determine the number of chunks needed
    num_chunks = (len(file_content) + CHUNK_SIZE - 1) // CHUNK_SIZE

    # Create a unique identifier for the file
    message_id = str(int(time.time() * 1000))  # Using timestamp as a unique identifier

    # Loop through the chunks and send each chunk as a separate message
    for i in range(num_chunks):
        start = i * CHUNK_SIZE
        end = (i + 1) * CHUNK_SIZE
        chunk_data = file_content[start:end]

        # Send the chunk as a file and get the message object
        file = discord.File(BytesIO(chunk_data), filename=f"{file_name}_part_{i + 1}")
        message = await channel.send(file=file)
        # Check the size of the file content in MB

        # Obtain the direct URL for the uploaded file
        file_url = message.attachments[0].url if message.attachments else None
        urls.append(file_url)
        # Insert the file name and message ID into the database
    cursor.execute("INSERT INTO fileinfo (file_name, message_id,file_split,file_url,size) VALUES (?, ?,?,?,?)", (file_name, message_id,"True",str(urls),str(updated_file_size) + " MB"))
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()

    return message_id


async def send_single_file(channel, file_content, file_name):
    # Create a new SQLite connection and cursor within the function
    conn = sqlite3.connect('genzfree.db')
    cursor = conn.cursor()

    # Send the file and get the message object
    file = discord.File(BytesIO(file_content), filename=file_name)
    message = await channel.send(file=file)

    # Obtain the direct URL for the uploaded file
    file_url = message.attachments[0].url if message.attachments else None

    # Insert the file URL and file name into the database
        # file_name text,
        #     file_url text,
        #     file_split text,
    file_size_mb = len(file_content) / (1024 * 1024)  
    updated_file_size = "{:.2f}".format(file_size_mb)
    cursor.execute("INSERT INTO fileinfo (file_name, file_url,file_split,message_id,size) VALUES (?, ?,?,?,?)", (file_name, file_url,"False","None",str(updated_file_size) +" MB"))
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()

    # You can use the file_url later for downloading
    return file_url



async def send_file_real(channel, file_content, file_name):
    if len(file_content) > CHUNK_SIZE:
        pass
        # If the file is larger than CHUNK_SIZE, use send_large_file
        return await send_large_file(channel, file_content, file_name)
    else:
        # Otherwise, send the file as a single message
        return await send_single_file(channel, file_content, file_name)


# Flask
@app.route('/download/<slog>')
def download(slog):
        conn = sqlite3.connect('genzfree.db')
        cursor = conn.cursor()
        cursor.execute(f"select * from fileinfo where message_id={slog}")
        data=cursor.fetchone()
        file__name = data[0]
        file_urls = data[1]

        url_list = ast.literal_eval(file_urls)

        merged_file = retrieve_and_merge_file(url_list)

        # Now, you can save the merged file to a local file or do further processing with it
        return send_file(merged_file, as_attachment=True, download_name=file__name)

@app.route('/', methods=['GET'])
def index():
    
    conn = sqlite3.connect('genzfree.db')
    cursor = conn.cursor()

    cursor.execute("select * from fileinfo")
    fileinfo = cursor.fetchall()
    return render_template('index.html',fileinfo=fileinfo,channel_id = CHANNEL_ID)

@app.route('/upload', methods=['POST'])
def upload():
    if request.method == "POST":
        if 'file' not in request.files:
            return 'No file part in the request'

        uploaded_file = request.files['file'].read()
        file_name = request.files['file'].filename

        channel = client.get_channel(CHANNEL_ID)
        if channel:
            client.loop.create_task(send_file_real(channel, uploaded_file, file_name))
            return redirect('/')
        else:
            return "OOPS CHANNEL NOT FOUND"

@app.route('/delete', methods=['POST'])
def delete():
    url = request.form.get('url')    
    try:
        conn = sqlite3.connect('genzfree.db')
        cursor = conn.cursor()

        # Use parameterized query to prevent SQL injection
        cursor.execute("DELETE FROM fileinfo WHERE file_url=?", (url,))
        conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"Error deleting record: {e}")

    finally:
        cursor.close()
        conn.close()

    # Redirect to the appropriate location
    return redirect('/?timestamp=' + str(time.time()))


@app.route('/delete/multiple', methods=['POST'])
def delete_multiple():
    msgid = request.form.get('msgid')    
    try:
        conn = sqlite3.connect('genzfree.db')
        cursor = conn.cursor()

        # Use parameterized query to prevent SQL injection
        cursor.execute("DELETE FROM fileinfo WHERE message_id=?", (msgid,))
        conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"Error deleting record: {e}")

    finally:
        cursor.close()
        conn.close()

    # Redirect to the appropriate location
    return redirect('/?timestamp=' + str(time.time()))

if __name__ == '__main__':
    flask_process = threading.Thread(target=run_flask)
    discord_thread = threading.Thread(target=run_discord)

    flask_process.start()
    discord_thread.start()

    flask_process.join()
    discord_thread.join()
