Installation:

Jeg har lavet et script, som installere databasen p� en given databaseserver. psql.exe og tilh�rende
.dll-filer er vedlagt, og installationen er testet p� en maskine uden PostgreSQL installeret.

Hvis der opleves fejl med psql, s� er det muligt at rette stien til C:\Program Files\PostgreSQL\10
i linie 5 i scriptet.

Hvis det ikke �nskes at importere eksempeldata ind i databasen kan linie 116 kommenteres ud.


Guides:

For anvendelse og funktionalitet henvises til f�lgende videoer:

	https://www.youtube.com/playlist?list=PLjALbFJFBcVmVgkiudju555xW8ivQfBhN

som gennemg�r indholdet af projektet.


Data Source:

Alle filer er sat til localhost, 5432.
Dette kan �ndres hhv. i QGIS-projekterne, samt i .odc-filerne i excel/connection.