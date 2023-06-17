import requests
from bs4 import BeautifulSoup
import pyodbc

server = 'DESKTOP-AJEQ8SM'
database = 'LightNovelDatabase'
username = 'TJTest'
password = 'Dragon122.'

connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};' + \
                    f'SERVER={server};' + \
                    f'DATABASE={database};' + \
                    f'UID={username};' + \
                    f'PWD={password}'

connection = pyodbc.connect(connection_string)

url = 'https://novelnext.org/sort/novelnext-popular'
response = requests.get(url)
html_content = response.content

soup = BeautifulSoup(html_content, 'html.parser')

books = []

for book_element in soup.find_all('div', class_='row'):
    # Extract book ID from source link
    source_link = book_element.find('a')['href']
    book_id = source_link.split('/')[-1].replace('.html', '')
    print(source_link)
    print(book_id)
    # Extract book title
    title_element = book_element.find('h3', class_='novel-title')
    if title_element:
        title = title_element.find('a')['title']
    else:
        title = None
    # Extract book author
    author_element = book_element.find('span', class_='author')
    author = author_element.text.strip() if author_element else None

    # Extract book cover image
    img_element = book_element.find('img')
    book_cover_image = img_element['src'] if img_element else None

    # Extract book publication date
    desc_element = book_element.find('div', class_='desc')
    publication_date = None
    if desc_element:
        for item_element in desc_element.find_all('div', class_='item'):
            if 'Published' in item_element.text:
                publication_date = item_element.find('span').text

    # Extract book genre
    genre = None
    if desc_element:
        for item_element in desc_element.find_all('div', class_='item'):
            if 'Genre' in item_element.text:
                genre = item_element.find('a').text

    # Extract book synopsis
    synop_element = book_element.find('div', class_='synopsys')
    synopsis = synop_element.find('div', class_='desc-text').text.strip() if synop_element else None

    # Add book data to list
    book_data = {
        'BookID': book_id,
        'Title': title,
        'Author': author,
        'PublicationDate': publication_date,
        'Genre': genre,
        'Synopsis': synopsis,
        'BookCoverImage': book_cover_image,
        'SourceLink': source_link
    }
    books.append(book_data)

# Insert or update book data in LightNovel table
try:
    with connection.cursor() as cursor:
        for book in books:
            sql = """
            MERGE INTO RecentlyAdded AS target
            USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?)) AS source (BookID, Title, Author, PublicationDate, Genre, Synopsis, BookCoverImage, SourceLink)
            ON target.BookID = source.BookID
            WHEN NOT MATCHED THEN
                INSERT (BookID, Title, Author, PublicationDate, Genre, Synopsis, BookCoverImage, SourceLink) 
                VALUES (source.BookID, source.Title, source.Author, source.PublicationDate, source.Genre, source.Synopsis, source.BookCoverImage, source.SourceLink);
            """
            params = (str(book['BookID']), book['Title'], book['Author'], book['PublicationDate'], book['Genre'], book['Synopsis'], book['BookCoverImage'], book['SourceLink'])
            cursor.execute(sql, params)
    connection.commit()
finally:
    connection.close()
