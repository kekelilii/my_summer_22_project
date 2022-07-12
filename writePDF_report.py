import warnings
from fpdf import FPDF
import time
import pandas as pd
import dataframe_image as dfi
import psycopg2 as pg
warnings.filterwarnings("ignore")

def data_generator(table):
    engine = pg.connect(database='my_db', user='postgres', password='Kelly1030', host='127.0.0.1', port='5432')
    df = pd.read_sql('select * from ' + table, con=engine)
    return df

def create_letterhead(pdf, WIDTH):
    pdf.image("kline_images/header.png", 0, 0, WIDTH)

def create_title(title, pdf):
    # Add main title
    pdf.set_font('Helvetica', 'b', 20)
    pdf.ln(40)
    pdf.write(5, title)
    pdf.ln(10)

    # Add date of report
    pdf.set_font('Helvetica', '', 14)
    pdf.set_text_color(r=128, g=128, b=128)
    today = time.strftime("%d/%m/%Y")
    pdf.write(4, f'{today}')

    # Add line break
    pdf.ln(10)

def create_subtitle(title, pdf):
    # Add main title
    pdf.set_font('Helvetica', 'b', 16)
    pdf.set_text_color(r=128, g=128, b=128)
    pdf.ln(30)
    pdf.write(5, title)
    # Add line break
    pdf.ln(10)

def write_to_pdf(pdf, words):
    # Set text colour, font size, and font type
    pdf.set_text_color(r=0, g=0, b=0)
    pdf.set_font('Times', '', 12)
    pdf.write(5, words)

class PDF(FPDF):
    def footer(self):
        self.set_y(-15)
        self.set_font('Helvetica', 'I', 8)
        self.set_text_color(128)
        self.cell(0, 10, str(self.page_no()), 0, 0, 'C')

def main():
    #Generate an image of data
    df = data_generator('kline_new')
    dfi.export(df, 'kline_images/kline_new.png', max_rows=15)

    df2 = data_generator('returns')
    dfi.export(df2, 'kline_images/returns.png', max_rows=8)
    cor = df2.corr(method='pearson')
    dfi.export(cor, 'kline_images/returns_corr.png')

    # Global Variables
    TITLE = "Binance Kline Data Report"
    WIDTH = 210
    HEIGHT = 297
    # Create PDF
    pdf = PDF()  # A4 (210 by 297 mm)
    '''
    First Page of PDF
    '''
    # Add Page
    pdf.add_page()
    # Add lettterhead and title
    create_letterhead(pdf, WIDTH)
    create_title(TITLE, pdf)
    create_subtitle("Summary", pdf)
    write_to_pdf(pdf, "        This database reports Kline data of virtual currencies supplied by Binance. Three tables are included: "
                      "Kline, Symbol, Vendor, and Returns. The Kline table records daily transaction data for each currency through "
                      "2021, with a total of 1,460 rows and no missing data. ")
    pdf.ln(15)
    # Add table
    pdf.image("kline_images/kline_new.png", w=185)
    pdf.ln(10)

    '''
    Second Page of PDF
    '''
    # Add Page
    pdf.add_page()
    # Add lettterhead
    create_letterhead(pdf, WIDTH)
    # Add some words to PDF
    create_subtitle("Charts", pdf)
    write_to_pdf(pdf, "1. The percentage of sum of trades (1: BTCUSDT 2: ETHUSDT 3: DOGEUSDT 4: LTCUSDT)")
    pdf.ln(10)
    write_to_pdf(pdf, "This pie chart displays the percentage of the number of trades of currencies. "
                      "We can see that BTC has the largest number of transactions, ETH and DOGE each account for about a quarter, "
                      "and LTC is the least.")
    pdf.ln(10)
    pdf.image("kline_images/of-trades-percentage-2022-07-06T23-21-48.035Z.jpg", w=160)
    pdf.ln(10)
    write_to_pdf(pdf, "2. Volume in January 2021 (1: BTCUSDT 2: ETHUSDT 3: DOGEUSDT 4: LTCUSDT)")
    pdf.ln(10)
    write_to_pdf(pdf, "This bar chart displays the daily trading volume of currencies in January 2021. "
                      "We can see the volume of DOGE is significantly higher than others.")
    '''
           Third Page of PDF
    '''
    # Add Page
    pdf.add_page()
    # Add lettterhead
    create_letterhead(pdf, WIDTH)
    # Add some words to PDF
    pdf.ln(30)
    pdf.image("kline_images/volume-jan-2022-07-06T23-50-16.908Z.jpg", w=160)
    pdf.ln(10)
    write_to_pdf(pdf, "3. Index")
    pdf.ln(10)
    write_to_pdf(pdf, "The following line charts show the four indexes of currencies and "
                      "their daily changes throughout 2021.")
    pdf.ln(10)
    write_to_pdf(pdf, "(1) BTCUSDT")
    pdf.ln(10)
    pdf.image("kline_images/btcusdt-index-2022-07-07T00-04-28.978Z.jpg", w=160)
    '''
               Forth Page of PDF
    '''
    # Add Page
    pdf.add_page()
    # Add lettterhead
    create_letterhead(pdf, WIDTH)
    pdf.ln(30)
    write_to_pdf(pdf, "(2) ETHUSDT")
    pdf.ln(10)
    pdf.image("kline_images/ethusdt-index-2022-07-07T00-04-44.457Z.jpg", w=160)
    pdf.ln(10)
    write_to_pdf(pdf, "(3) DOGEUSDT")
    pdf.ln(10)
    pdf.image("kline_images/dogeusdt-index-2022-07-07T00-04-57.756Z.jpg", w=160)
    '''
                   Fifth Page of PDF
    '''
    # Add Page
    pdf.add_page()
    # Add lettterhead
    create_letterhead(pdf, WIDTH)
    pdf.ln(30)
    write_to_pdf(pdf, "(4) LTCUSDT")
    pdf.ln(10)
    pdf.image("kline_images/ltcusdt-index-2022-07-07T00-05-09.048Z.jpg", w=160)
    pdf.ln(10)
    write_to_pdf(pdf, "4. Returns")
    pdf.ln(10)
    write_to_pdf(pdf, "The following line charts show the returns of currencies. We can see their volatility")
    pdf.ln(10)
    write_to_pdf(pdf, "(1) BTCUSDT")
    pdf.ln(10)
    pdf.image("kline_images/btc-return-2022-07-07T00-25-37.116Z.jpg", w=160)
    '''
                       Sixth Page of PDF
    '''
    pdf.add_page()
    # Add lettterhead
    create_letterhead(pdf, WIDTH)
    pdf.ln(30)
    write_to_pdf(pdf, "(2) ETHUSDT")
    pdf.ln(10)
    pdf.image("kline_images/eth-return-2022-07-07T00-25-45.193Z.jpg", w=160)
    pdf.ln(10)
    write_to_pdf(pdf, "(3) DOGEUSDT")
    pdf.ln(10)
    pdf.image("kline_images/doge-return-2022-07-07T00-25-58.109Z.jpg", w=160)
    '''
                           Seventh Page of PDF
    '''
    pdf.add_page()
    # Add lettterhead
    create_letterhead(pdf, WIDTH)
    pdf.ln(30)
    write_to_pdf(pdf, "(4) LTCUSDT")
    pdf.ln(10)
    pdf.image("kline_images/ltc-return-2022-07-07T00-26-08.314Z.jpg", w=160)
    create_subtitle("Returns", pdf)
    pdf.image("kline_images/returns.png", w=120)
    '''
                               Eighth Page of PDF
        '''
    pdf.add_page()
    # Add letterhead
    create_letterhead(pdf, WIDTH)
    pdf.ln(30)
    write_to_pdf(pdf, "Correlation between the returns of the currencies.")
    pdf.ln(10)
    pdf.image("kline_images/returns_corr.png", w=110)

    # Generate the PDF
    pdf.output("kline_report.pdf", 'F')

if __name__ == '__main__':
    main()