import urllib.request
import xml.etree.ElementTree as ET
import pandas as pd
import time

data = pd.read_csv("dbt/models/proximity/raw_download.csv")

data = pd.DataFrame(data)


apt_variations = [
    "1a",
    "1b",
    "2a",
    "2b",
    "3a",
    "3b",
    "4a",
    "4b",
    "5a",
    "5b",
    "1",
    "2",
    "3",
    "4",
    "5",
    "6",
    "A",
    "B",
    "C",
    "D",
    "E",
    "F",
    "G",
    "1N",
    "1S",
    "2N",
    "2S",
    "3N",
    "3S",
    "1E",
    "1W",
    "2E",
    "2W",
    "3E",
    "3W",
    "Basement",
    "Garden",
    "BSMT",
]

requestXML_template = """
<?xml version="1.0"?>
<AddressValidateRequest USERID="6Z890COOKC697">
    <Revision>1</Revision>
    <Address ID="0">
        <Address1>{address1}</Address1>
        <Address2>{address2}</Address2>
        <City>{city}</City>
        <State>{state}</State>
        <Zip5>{zip5}</Zip5>
        <Zip4></Zip4>
    </Address>
</AddressValidateRequest>
"""

# Create or overwrite the output file and write the header
output_file = "output.csv"
header_written = False

# Loop through each row in the DataFrame
for idx, row in data.iterrows():
    address1 = f"{row['prop_address_street_number']} {row['prop_address_street_dir']} {row['prop_address_street_name']} {row['prop_address_suffix_1']}"
    city = row["prop_address_city_name"]
    state = row["prop_address_state"]
    zip5 = row["prop_address_zipcode_1"]

    for apt in apt_variations:
        requestXML = requestXML_template.format(
            address1=address1, address2=apt, city=city, state=state, zip5=zip5
        )

        docString = requestXML.replace("\n", "").replace("\t", "")
        docString = urllib.parse.quote_plus(docString)

        url = (
            "http://production.shippingapis.com/ShippingAPI.dll?API=Verify&XML="
            + docString
        )
        print("Requesting:", url)

        response = urllib.request.urlopen(url)
        if response.getcode() != 200:
            print("Error making HTTP call:")
            print(response.info())
            exit()

        contents = response.read().decode("utf-8")
        root = ET.fromstring(contents)

        for address in root.findall("Address"):
            address1_text = (
                address.find("Address1").text
                if address.find("Address1") is not None
                else ""
            )
            address2_text = (
                address.find("Address2").text
                if address.find("Address2") is not None
                else ""
            )
            city_text = (
                address.find("City").text
                if address.find("City") is not None
                else ""
            )
            state_text = (
                address.find("State").text
                if address.find("State") is not None
                else ""
            )
            zip5_text = (
                address.find("Zip5").text
                if address.find("Zip5") is not None
                else ""
            )
            zip4_text = (
                address.find("Zip4").text
                if address.find("Zip4") is not None
                else ""
            )
            delivery_point = (
                address.find("DeliveryPoint").text
                if address.find("DeliveryPoint") is not None
                else ""
            )
            carrier_route = (
                address.find("CarrierRoute").text
                if address.find("CarrierRoute") is not None
                else ""
            )
            footnotes = (
                address.find("Footnotes").text
                if address.find("Footnotes") is not None
                else ""
            )
            dpv_confirmation = (
                address.find("DPVConfirmation").text
                if address.find("DPVConfirmation") is not None
                else ""
            )
            dpv_cmra = (
                address.find("DPVCMRA").text
                if address.find("DPVCMRA") is not None
                else ""
            )
            dpv_false = (
                address.find("DPVFalse").text
                if address.find("DPVFalse") is not None
                else ""
            )
            dpv_footnotes = (
                address.find("DPVFootnotes").text
                if address.find("DPVFootnotes") is not None
                else ""
            )
            business = (
                address.find("Business").text
                if address.find("Business") is not None
                else ""
            )
            central_delivery_point = (
                address.find("CentralDeliveryPoint").text
                if address.find("CentralDeliveryPoint") is not None
                else ""
            )
            vacant = (
                address.find("Vacant").text
                if address.find("Vacant") is not None
                else ""
            )

            full_address = f"{address1_text} {address2_text}, {city_text}, {state_text} {zip5_text}-{zip4_text}".strip().replace(
                " ,", ","
            )

            result_8_29 = {
                "PIN": row["pin"],
                "Full Address": full_address,
                "Address1": address1_text,
                "Address2": address2_text,
                "City": city_text,
                "State": state_text,
                "Zip5": zip5_text,
                "Zip4": zip4_text,
                "DeliveryPoint": delivery_point,
                "CarrierRoute": carrier_route,
                "Footnotes": footnotes,
                "DPVConfirmation": dpv_confirmation,
                "DPVCMRA": dpv_cmra,
                "DPVFalse": dpv_false,
                "DPVFootnotes": dpv_footnotes,
                "Business": business,
                "CentralDeliveryPoint": central_delivery_point,
                "Vacant": vacant,
                "Root": ET.tostring(root, encoding="unicode", method="xml"),
                "Contents": contents,
            }

            # Append the result to the output CSV file
            df = pd.DataFrame([result_8_29])
            df.to_csv(
                output_file, mode="a", index=False, header=not header_written
            )
            header_written = True

        time.sleep(0.25)

print("Processing complete.")
