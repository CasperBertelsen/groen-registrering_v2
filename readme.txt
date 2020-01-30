Installation:

Jeg har lavet et script, som installerer databasen på en given databaseserver. psql.exe og tilhørende
.dll-filer er vedlagt, og installationen er testet på en maskine uden PostgreSQL installeret.

Hvis der opleves fejl med psql, så er det muligt at rette stien til C:\Program Files\PostgreSQL\10
i linie 5 i scriptet.

Hvis det ikke ønskes at importere eksempeldata ind i databasen kan linie 116 kommenteres ud.


Guides:

For anvendelse og funktionalitet henvises til følgende videoer:

	https://www.youtube.com/playlist?list=PLjALbFJFBcVmVgkiudju555xW8ivQfBhN

som gennemgår indholdet af projektet.


Data Source:

Alle filer er sat til localhost, 5432.
Dette kan ændres hhv. i QGIS-projekterne, samt i .odc-filerne i excel/connection.
