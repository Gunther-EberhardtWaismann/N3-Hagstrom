# soap_service.py
# ----------------------------
# BACKEND COMPLETO – SERVIDOR SOAP EM PYTHON
# Compatível com SOAPUI
#
# Instalar dependências:
#   pip install spyne flask pandas gevent
#
# Rodar:
#   python soap_service.py
#
# WSDL:
#   http://localhost:8000/?wsdl
# ----------------------------

from spyne import Application, rpc, ServiceBase, Integer, Unicode, Double, Iterable, ComplexModel
from spyne.protocol.soap import Soap11
from spyne.server.wsgi import WsgiApplication
from flask import Flask, Response, request
import pandas as pd

CSV_PATH = 'SCMS_Delivery_History_Dataset.csv'

_df_cache = None
def load_df():
    global _df_cache
    if _df_cache is None:
        _df_cache = pd.read_csv(CSV_PATH, dtype=str)
    return _df_cache


class Delivery(ComplexModel):
    __namespace__ = 'tns'

    ID = Unicode
    ProjectCode = Unicode
    PQ = Unicode
    PO_SO = Unicode
    ASN_DN = Unicode
    Country = Unicode
    ManagedBy = Unicode
    FulfillVia = Unicode
    Vendor = Unicode
    INCO_Term = Unicode
    ShipmentMode = Unicode
    LineItemValue = Unicode
    WeightKg = Unicode
    FreightCostUSD = Unicode


class Statistics(ComplexModel):
    __namespace__ = 'tns'

    total_deliveries = Integer
    total_line_item_value = Double
    total_weight_kg = Double
    total_freight_usd = Double


class SCMSService(ServiceBase):

    @rpc(Integer, _returns=Delivery)
    def getDeliveryByID(ctx, id):
        df = load_df()
        row = df[df['ID'].astype(int) == int(id)]
        if row.empty:
            return Delivery(ID=str(id))

        r = row.iloc[0].to_dict()
        return Delivery(
            ID=str(r.get('ID', '')),
            ProjectCode=str(r.get('Project Code', '')),
            PQ=str(r.get('PQ #', '')),
            PO_SO=str(r.get('PO / SO #', '')),
            ASN_DN=str(r.get('ASN/DN #', '')),
            Country=str(r.get('Country', '')),
            ManagedBy=str(r.get('Managed By', '')),
            FulfillVia=str(r.get('Fulfill Via', '')),
            Vendor=str(r.get('Vendor', '')),
            INCO_Term=str(r.get('INCO Term', '')),
            ShipmentMode=str(r.get('Shipment Mode', '')),
            LineItemValue=str(r.get('Line Item Value', '')),
            WeightKg=str(r.get('Weight (Kilograms)', '')),
            FreightCostUSD=str(r.get('Freight Cost (USD)', ''))
        )

    @rpc(Unicode, _returns=Iterable(Delivery))
    def getDeliveriesByCountry(ctx, country):
        df = load_df()
        rows = df[df['Country'].str.lower() == country.lower()]
        for _, r in rows.iterrows():
            yield Delivery(
                ID=str(r.get('ID', '')),
                ProjectCode=str(r.get('Project Code', '')),
                PQ=str(r.get('PQ #', '')),
                PO_SO=str(r.get('PO / SO #', '')),
                ASN_DN=str(r.get('ASN/DN #', '')),
                Country=str(r.get('Country', '')),
                ManagedBy=str(r.get('Managed By', '')),
                FulfillVia=str(r.get('Fulfill Via', '')),
                Vendor=str(r.get('Vendor', '')),
                INCO_Term=str(r.get('INCO Term', '')),
                ShipmentMode=str(r.get('Shipment Mode', '')),
                LineItemValue=str(r.get('Line Item Value', '')),
                WeightKg=str(r.get('Weight (Kilograms)', '')),
                FreightCostUSD=str(r.get('Freight Cost (USD)', ''))
            )

    @rpc(Unicode, _returns=Iterable(Delivery))
    def getDeliveriesByVendor(ctx, vendor):
        df = load_df()
        rows = df[df['Vendor'].str.lower() == vendor.lower()]
        for _, r in rows.iterrows():
            yield Delivery(
                ID=str(r.get('ID', '')),
                ProjectCode=str(r.get('Project Code', '')),
                PQ=str(r.get('PQ #', '')),
                PO_SO=str(r.get('PO / SO #', '')),
                ASN_DN=str(r.get('ASN/DN #', '')),
                Country=str(r.get('Country', '')),
                ManagedBy=str(r.get('Managed By', '')),
                FulfillVia=str(r.get('Fulfill Via', '')),
                Vendor=str(r.get('Vendor', '')),
                INCO_Term=str(r.get('INCO Term', '')),
                ShipmentMode=str(r.get('Shipment Mode', '')),
                LineItemValue=str(r.get('Line Item Value', '')),
                WeightKg=str(r.get('Weight (Kilograms)', '')),
                FreightCostUSD=str(r.get('Freight Cost (USD)', ''))
            )

    @rpc(Unicode, _returns=Iterable(Delivery))
    def getDeliveriesByShipmentMode(ctx, shipment_mode):
        df = load_df()
        rows = df[df['Shipment Mode'].str.lower() == shipment_mode.lower()]
        for _, r in rows.iterrows():
            yield Delivery(
                ID=str(r.get('ID', '')),
                ProjectCode=str(r.get('Project Code', '')),
                PQ=str(r.get('PQ #', '')),
                PO_SO=str(r.get('PO / SO #', '')),
                ASN_DN=str(r.get('ASN/DN #', '')),
                Country=str(r.get('Country', '')),
                ManagedBy=str(r.get('Managed By', '')),
                FulfillVia=str(r.get('Fulfill Via', '')),
                Vendor=str(r.get('Vendor', '')),
                INCO_Term=str(r.get('INCO Term', '')),
                ShipmentMode=str(r.get('Shipment Mode', '')),
                LineItemValue=str(r.get('Line Item Value', '')),
                WeightKg=str(r.get('Weight (Kilograms)', '')),
                FreightCostUSD=str(r.get('Freight Cost (USD)', ''))
            )

    @rpc(_returns=Statistics)
    def getStatistics(ctx):
        df = load_df()

        def to_float(col):
            try:
                return pd.to_numeric(df[col].str.replace(',', ''), errors='coerce').fillna(0).sum()
            except:
                return 0

        return Statistics(
            total_deliveries=len(df),
            total_line_item_value=float(to_float('Line Item Value')),
            total_weight_kg=float(to_float('Weight (Kilograms)')),
            total_freight_usd=float(to_float('Freight Cost (USD)'))
        )

    @rpc(_returns=Iterable(Unicode))
    def listCountries(ctx):
        df = load_df()
        for c in sorted(df['Country'].dropna().unique()):
            yield str(c)

    @rpc(_returns=Iterable(Unicode))
    def listVendors(ctx):
        df = load_df()
        for v in sorted(df['Vendor'].dropna().unique()):
            yield str(v)


app = Flask(__name__)

soap_app = Application(
    [SCMSService],
    'tns',
    in_protocol=Soap11(validator='lxml'),
    out_protocol=Soap11()
)

wsgi_app = WsgiApplication(soap_app)


@app.route('/', methods=['GET', 'POST'])
def soap_server():
    if request.method == 'GET':
        return Response(soap_app.get_interface_document(), mimetype='text/xml')
    else:
        response = wsgi_app(request.environ, lambda status, headers: None)
        return Response(response[0], mimetype='text/xml')


if __name__ == '__main__':
    print("SOAP Server rodando em http://localhost:8000/?wsdl")
    from gevent.pywsgi import WSGIServer
    WSGIServer(('0.0.0.0', 8000), app).serve_forever()
