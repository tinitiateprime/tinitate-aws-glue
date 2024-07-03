from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import io

def create_pdf(data, buffer):
    # Initialize a canvas with a specified buffer and page size
    c = canvas.Canvas(buffer, pagesize=letter)
    # Retrieve the width and height for the page
    width, height = letter
    # Draw strings at specified coordinates on the PDF, using data from the 'data' dictionary
    c.drawString(100, height - 100, f"Name: {data['name']}")
    c.drawString(100, height - 120, f"Age: {data['age']}")
    c.drawString(100, height - 140, f"City: {data['city']}")
    # Save the canvas changes to the buffer
    c.save()
    # Reset the buffer pointer to the start for subsequent read or write operations
    buffer.seek(0)

def main():
    # Example data dictionary
    sample_data = {'name': 'Alice', 'age': 30, 'city': 'New York'}
    # Create a bytes buffer for PDF generation
    pdf_buffer = io.BytesIO()
    # Generate PDF with sample data and store in buffer
    create_pdf(sample_data, pdf_buffer)

    # Add code here to upload `pdf_buffer` to S3 if needed

if __name__ == '__main__':
    main()
