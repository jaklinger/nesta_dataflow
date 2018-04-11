from utils.common.browser import SelfClosingBrowser
from utils.common.datapipeline import DataPipeline
from selenium.webdriver.common.action_chains import ActionChains
import time


def get_data(b, ROW, CELL, ITEM, ABOUT):
    output = []
    columns = []
    # Need to iterate over index as the reference goes stale
    n_rows = len(b.find_elements_by_class_name(ROW))
    for ir in range(0, n_rows):
        rows = b.find_elements_by_class_name(ROW)
        row = rows[ir]
        cells = row.find_elements_by_class_name(CELL)
        if not (len(columns) == 0 or len(columns) == len(cells)):
            continue
        data_row = {}
        for ic, cell in enumerate(cells):
            # Collect column names if first row
            if ir == 0:
                col_name = cell.text.replace(" ", "_").replace("^", "").lower()
                columns.append(col_name)
            # Otherwise map the column names to row data
            elif cell.text.strip() != "":
                data_row[columns[ic]] = cell.text

        # Don't try to access extra data for the header row
        if data_row == {}:
            continue
        # If this is a data row, click to reveal
        time.sleep(2)
        ActionChains(b).move_to_element(row).click().perform()
        time.sleep(1)

        # Get the "About" text
        div = b.find_element_by_xpath(ABOUT.format(str(3*(ir+1)/2)))
        if len(div.text.split()) > 10 and not div.text.startswith("DETAILS"):
            data_row['about'] = div.text

        # Then get hidden data
        address = []
        for item in b.find_elements_by_class_name(ITEM):
            if item.text.strip() == "":
                continue
            # Extract ":" delimited data
            if ":" in item.text:
                field, value = item.text.split(":")
                col_name = field.replace(" ", "_").replace("^", "").lower()
                if col_name not in ["title", "amount_awarded",
                                    "awarded", "awarded_on",
                                    "grant_number", "location",
                                    "organization", "page",
                                    "timeframe", "topics",
                                    "website", "year",
                                    "about", "address"]:
                    continue
                value = value.lstrip().rstrip()
                #                print(col_name, value)
                # print(col_name, value)
                data_row[col_name] = value
            # Website means stop collecting, since personal
            # data beyond this
            elif item.text == "Website":
                anchor = item.find_element_by_tag_name("a")
                data_row["website"] = anchor.get_attribute("href")
                break
            # Otherwise data is address
            else:
                address.append(item.text)
        ActionChains(b).move_to_element(row).click().perform()
        #print("\n")

        # Finally, get grantee name
        bold_text = []
        for item in b.find_elements_by_tag_name("strong"):
            if item.text != "":
                bold_text.append(item.text)
        contacts = bold_text[5: len(bold_text) - 2]

        # Join any lists
        data_row["address"] = "\n".join(address)
        data_row["contacts"] = "\n".join(contacts)
        output.append(data_row)
    return output


def run(config):
    # Parameters
    top_url = config["parameters"]["src"]
    ROW = "grants-database__table-row"
    CELL = "grants-database__table-cell"
    ITEM = "grants-database__item-wrapper"
    ABOUT = '//*[@id="grants-table"]/div[2]/table/tbody/tr[{}]/td/div/div/div/div[1]/div'
    max_page = 3400
    min_year = 1997
    chunk_limit = 50

    # Outputs
    output = []
    ipage = 82
    years = set((3000,)) # at least one year
    while ipage < max_page:
        with SelfClosingBrowser(top_url=top_url, headless=False) as b:
            for page in range(ipage, max_page):
                #if page >= 130 and page < 2359:
                #    continue
                b.get(top_url+"#s="+str(ipage))
                time.sleep(3)
                _output = get_data(b, ROW, CELL, ITEM, ABOUT)
                # Check output is sensible
                do_break = False
                for row in _output:
                    # If it isn't self-consistent then break out and try again
                    if (("amount_awarded" not in row or
                         row["awarded"] != row["amount_awarded"])):
                        print(row)
                        print("--> Going to retry page", page)
                        do_break = True
                        break
                if do_break:
                    time.sleep(10)
                    break
                # If data is good, increment parameters
                ipage += 1
                output += _output
                # Add the page number to the output
                for row in output:
                    if "page" not in row:
                        row["page"] = page
                    years.add(int(row["year"]))
                # Write data if it reaches the chunk limit
                if len(output) >= chunk_limit:
                    break
            if len(output) > 0:
                with DataPipeline(config, write_google=True) as dp:
                    for row in output:
                        dp.insert(row)
                output = []
        if min(years) <= min_year:
            break

    # Write the output
    with DataPipeline(config, write_google=True) as dp:
        for row in output:
            dp.insert(row)


if __name__ == "__main__":
    run()
