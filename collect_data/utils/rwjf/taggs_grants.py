from utils.common.browser import SelfClosingBrowser
from utils.common.datapipeline import DataPipeline
import time


def run(config):
    # Parameters
    top_url = config["parameters"]["src"]

    # Output = list of dicts
    output = []
    with SelfClosingBrowser(top_url=top_url, headless=False) as b:
        b.get(top_url)
        # Do something and fill output
        time.sleep(3)

    # Write the output
    with DataPipeline(config, write_google=True) as dp:
        for row in output:
            dp.insert(row)


if __name__ == "__main__":
    run()
