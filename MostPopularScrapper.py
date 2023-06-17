import datetime
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

base_url = 'https://novelnext.org'

books = []

# Scrape book list page
for page in range(142,143):
    print(page)
    url = f'https://novelnext.org/sort/novelnext-popular?page={page}'
    response = requests.get(url)
    html_content = response.content
    soup = BeautifulSoup(html_content, 'html.parser')

    # Find individual book links and visit each page to scrape book details
    counter = 0
    for book_element in soup.find_all('div', class_='row'):
        if counter == 0:
            counter += 1
            continue
        title_element = book_element.find('h3', class_='novel-title')
        if not title_element or not title_element.find('a'):
            # If no source link is found, break out of the loop and move on to the next page
            break
        # Extract book ID from source link
        source_link_element = title_element.find('a')
        source_link = source_link_element['href']
        book_id = source_link.split('/')[-1].replace('.html', '')

        # Visit individual book page to extract book details
        book_url = f'{base_url}/novelnext/{book_id}'
        book_response = requests.get(book_url)
        book_html_content = book_response.content
        book_soup = BeautifulSoup(book_html_content, 'html.parser')
                
        # Extract book title
        title_element = book_soup.find('div', class_='desc').find('h3', class_='title')
        title = title_element.text.strip() if title_element else None
        title = title.encode('utf-8', 'ignore').decode('utf-8')

        
       # Extract book author
        author_element = book_soup.find('h3', text='Author:')
        if not author_element:
            author_element = book_soup.find('li', text='Author:')
        if author_element:
            author = author_element.find_next('a').text.strip()
        else:
            author = None


        # Extract book cover image
        img_element = book_soup.find('div', class_='book').find('img')
        book_cover_image = img_element['src'] if img_element else None


        # Extract book publication date
        publication_date_element = book_soup.find('div', class_='novel-update-date')
        publication_date = publication_date_element.text.strip() if publication_date_element else None

        # Extract ratings
        rating_element = book_soup.find('span', itemprop='ratingValue')
        rating = float(rating_element.text) if rating_element else None
    

        # Extract number of ratings
        num_ratings_element = book_soup.find('span', itemprop='reviewCount')
        num_ratings = int(num_ratings_element.text.replace(',', '')) if num_ratings_element else None
        
        # Extract book genres
        genre_element = book_soup.find('h3', string='Genre:')
        genres = []
        if genre_element:
            genre_links = genre_element.find_next_siblings('a')
            if genre_links:
                for genre_link in genre_links:
                    genres.append(genre_link.text)

        # Extract book status
        status_element = book_soup.find('h3', string='Status:')
        if status_element:
            status_link = status_element.find_next_sibling('a')
            if status_link:
                book_status = status_link.text
            else:
                book_status = None
        else:
            book_status = None
        


        # Extract book synopsis
        synopsis_element = book_soup.find('div', id='tab-description')
        synopsis = synopsis_element.find('div', class_='desc-text').text.strip() if synopsis_element else None
        
        # Add book data to list
        genres_str = ', '.join(genres)
        book_data = {
            'BookID': book_id,
            'Title': title,
            'Author': author,
            'PublicationDate': publication_date,
            'Genre': genres_str,
            'Synopsis': synopsis,
            'BookCoverImage': book_cover_image,
            'SourceLink': book_url,
            'Ratings': rating,
            'NumRatings': num_ratings,
            'Status': book_status
        }
        books.append(book_data)

# Insert or update book data in MostPopular table
try:
    with connection.cursor() as cursor:
        for book in books:
            if book['Title'] and book['BookCoverImage']:
                sql = """
                MERGE INTO MostPopular AS target
                USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source (BookID, Title, Author, PublicationDate, Genre, Synopsis, BookCoverImage, SourceLink, Ratings, NumRatings, Status)
                ON target.BookID = source.BookID
                WHEN NOT MATCHED THEN
                INSERT (BookID, Title, Author, PublicationDate, Genre, Synopsis, BookCoverImage, SourceLink, Ratings, NumRatings, Status) 
                VALUES (source.BookID, source.Title, source.Author, source.PublicationDate, source.Genre, source.Synopsis, source.BookCoverImage, source.SourceLink, source.Ratings, source.NumRatings, source.Status);
                """
                params = (str(book['BookID']), book['Title'], book['Author'], book['PublicationDate'], book['Genre'], book['Synopsis'], book['BookCoverImage'], book['SourceLink'],book['Ratings'], book['NumRatings'], book['Status'])
                cursor.execute(sql, params)
    connection.commit()
finally:
    connection.close()
