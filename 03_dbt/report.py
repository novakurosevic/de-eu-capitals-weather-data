from google.cloud import bigquery, storage
from google.api_core.exceptions import GoogleAPIError
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import KeepTogether
from reportlab.platypus import TableStyle
from reportlab.lib import colors
from reportlab.lib.enums import TA_JUSTIFY
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfgen import canvas
from io import BytesIO
from pathlib import Path
import time
import os

# Check are config files set.
config_directory = Path("/app/config")
required_files = ["config.json", "credentials.json"]

missing_files = []

for file_name in required_files:
    if not (config_directory / file_name).exists():
        missing_files.append(file_name)

if missing_files:
    raise FileNotFoundError(
        f"Missing required configuration files: {missing_files}"
    )


# Enums
PROJECT_ID = os.environ.get("DBT_GCP_PROJECT")
DATASET = os.environ.get("DBT_DATASET")

BUCKET_NAME = os.environ.get("GCS_BUCKET")
DESTINATION_PATH = os.environ.get("GCS_REPORT_PATH", "reports/report.pdf")


MODELS = [
    ("top_10_max_temp_capitals", "Top 10 Highest Temperatures In Capitals"),
    ("top_10_min_temp_capitals", "Top 10 Lowest Temperatures In Capitals"),
    ("top_10_sunniest_period_by_capital", "Top 10 Sunniest Periods by Capital"),
    ("top_10_foggiest_period_by_capital", "Top 10 Foggiest Periods by Capital"),
    ("top_10_foggiest_towns", "Top 10 Foggiest Towns"),
    ("top_10_rainiest_capitals_by_amount", "Top 10 Rainiest Capitals (Amount)"),
    ("top_10_rainiest_period_by_capital", "Top 10 Rainiest Periods by Capital"),
    ("top_10_dryest_period_by_capital", "Top 10 Dryest Periods by Capital"),
    ("top_10_snowest_capitals", "Top 10 Snowiest Capitals"),
    ("top_10_snowest_period_by_capital", "Top 10 Snowiest Periods by Capital"),
    ("weather_station_constant_work_days_per_capital", "Weather Station Consistency")
]

# Functions
def format_column(name):
    return name.replace("_", " ").title()


class NumberedCanvas(canvas.Canvas):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._saved_page_states = []

    def showPage(self):
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        total_pages = len(self._saved_page_states)

        for state in self._saved_page_states:
            self.__dict__.update(state)
            self.draw_page_number(total_pages)
            super().showPage()

        super().save()

    def draw_page_number(self, total_pages):
        self.setFont("Helvetica", 9)
        self.drawRightString(
            550, 20,
            f"Page {self._pageNumber} / {total_pages}"
        )

client = bigquery.Client(project=PROJECT_ID)
storage_client = storage.Client()

styles = getSampleStyleSheet()

# Use buffer output
buffer = BytesIO()
doc = SimpleDocTemplate(
    buffer,
    leftMargin=40,
    rightMargin=40,
    topMargin=40,
    bottomMargin=40
)

story = []

import time

def run_query(table_name, retries=3):
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.{table_name}`
    """

    for attempt in range(retries):
        attempt_num = attempt + 1

        print(f"[DBT][BigQuery] Attempt {attempt_num}/{retries} for table: {table_name}")

        try:
            result = client.query(query).result()
            print(f"[DBT][BigQuery] Success on attempt {attempt_num}")
            return result

        except Exception as e:
            print(f"[DBT][BigQuery] Error on attempt {attempt_num}: {e}")

            if attempt < retries - 1:
                wait = 2 ** attempt
                print(f"[DBT][BigQuery] Retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"[DBT][BigQuery] Query failed after {retries} attempts")
                raise Exception(f"[DBT] Query failed after {retries} attempts") from e

def format_row(row):
    return ", ".join([f"{k}: {v}" for k, v in row.items()])

# Build content

# Title
story.append(Paragraph("Weather Report", styles["Heading1"]))
story.append(Spacer(1, 12))

# Text
intro_text = """
Weather data in this project is sourced from historical records that start from <b>01 January 1970</b>.
However, data availability is not uniform across all cities and time periods.
<br/>
• Some cities have <b>continuous data from 1970 to 2025</b><br/>
• Other cities only have data starting from later years (e.g. 2010–2025)<br/><br/>

Additionally, even when data exists for a given period, it may be <b>incomplete</b>:<br/><br/>

• Core metrics such as <b>temperature, minimum temperature, and maximum temperature</b> are generally available<br/>
• Other metrics (such as <b>snow, precipitation, humidity, wind, etc.</b>) are optional and may be missing<br/>
• Some weather stations start reporting additional metrics only after a certain year<br/><br/>

Because of these inconsistencies, results should be interpreted with caution.<br/>
This does not reflect data inaccuracy, but rather <b>uneven historical coverage across locations and time periods</b>.
"""

text_style = ParagraphStyle(
    name="Justify",
    parent=styles["Normal"],
    alignment=TA_JUSTIFY
)

story.append(Paragraph(intro_text, text_style))
story.append(Spacer(1, 12))

# Tables
for model, title in MODELS:
    block = []

    # Convert to list
    rows = list(run_query(model))  

    if not rows:
        story.append(Paragraph("No data available", styles["Normal"]))
        story.append(Spacer(1, 10))
        continue

    # Header
    headers = [format_column(h) for h in rows[0].keys()]
    raw_headers = list(rows[0].keys())

    table_data = []
    table_data.append(headers)

    # Rows
    for row in rows:
        table_data.append([str(row[h]) for h in raw_headers])

    table = Table(table_data, hAlign='LEFT')

    table.setStyle(TableStyle([
        # Table Header
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#4F81BD")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),

        # Table Body
        ("BACKGROUND", (0, 1), (-1, -1), colors.HexColor("#F7F9FC")),

        # Table Grid
        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#D0D7E5")),

        # Table Padding
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
    ]))

    block.append(Paragraph(title, styles["Heading2"]))
    block.append(Spacer(1, 8))
    block.append(table)

    story.append(KeepTogether(block))
    story.append(Spacer(1, 20))

# Build PDF to buffer
doc.build(story, canvasmaker=NumberedCanvas)

# Set pointer to start
buffer.seek(0)

# Upload from buffer
bucket = storage_client.bucket(BUCKET_NAME)
blob = bucket.blob(DESTINATION_PATH)

blob.upload_from_file(buffer, content_type="application/pdf")

print(f"[DBT] Uploaded to gs://{BUCKET_NAME}/{DESTINATION_PATH}")
