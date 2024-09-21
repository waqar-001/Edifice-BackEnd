pip install Flask pandas numpy openpyxl


Testing with Postman:
URL: http://localhost:5000/process
Method: POST
Body: Select raw and use JSON format.
Example Body:
json
{
    "file_name": "sample.xlsx",
    "sheet_name": "Sheet1"
}
