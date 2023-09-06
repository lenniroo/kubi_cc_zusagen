# outbound-callcenter-zusagen

Gathers commitments for outbound sales from external callcenter agencies via sftp and writes them to DWH-Database hourly. Also every month a template with the current campaigns is send out to the external callcenters via the same sftp.

Before running the application please install required libraries by executing

    pip install -r requirements.txt

You also need a .env file in the folders get_ep_zusagen and generate_template.