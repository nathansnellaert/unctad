"""Fetch UNCTAD datasets from UNCTADStat API.

Downloads bulk data files for all configured datasets and saves as parquet.
"""
import io
import os
import tempfile
import py7zr
import pyarrow as pa
import pyarrow.csv as pac
from subsets_utils import get, save_raw_parquet


# Mapping from local asset names to UNCTAD API report codes
DATASET_MAPPING = {
    "associated_plastics_trade": "US.AssociatedPlasticsTradebyPartner",
    "biotrade_concentration": "US.BioTradeMerchProdConcent",
    "biotrade_gdp_share": "US.BioTradeMerchGDPShare",
    "biotrade_market_indices": "US.BioTradeMerchMarketIndices",
    "biotrade_merchandise": "US.BiotradeMerch",
    "biotrade_rca": "US.BiotradeMerchRCA",
    "biotrade_share": "US.BiotradeMerchShare",
    "commodity_price_indices_annual": "US.CommodityPriceIndices_A",
    "commodity_price_indices_monthly": "US.CommodityPriceIndices_M",
    "commodity_prices_annual": "US.CommodityPrice_A",
    "commodity_prices_monthly": "US.CommodityPrice_M",
    "consumer_price_index_annual": "US.Cpi_A",
    "container_port_throughput": "US.ContPortThroughput",
    "creative_goods_growth_rates": "US.CreativeGoodsGR",
    "creative_goods_index": "US.CreativeGoodsIndex",
    "creative_goods_value": "US.CreativeGoodsValue",
    "creative_services_group_e": "US.CreativeServ_Group_E",
    "creative_services_individual": "US.CreativeServ_Indiv_Tot",
    "current_account_balance": "US.CurrAccBalance",
    "digitally_deliverable_services": "US.DigitallyDeliverableServices",
    "exchange_rates": "US.ExchangeRateCrosstab",
    "fdi_flows_and_stock": "US.FdiFlowsStock",
    "fleet_beneficial_owners": "US.FleetBeneficialOwners",
    "food_trade_by_processing": "US.TradeFoodCatByProc",
    "food_trade_processing_by_category": "US.TradeFoodProcByCat",
    "food_trade_rca_by_category": "US.TradeFoodProcCat_Cat_RCA",
    "food_trade_rca_by_processing": "US.TradeFoodProcCat_Proc_RCA",
    "gdp_by_expenditure_and_activity": "US.GDPComponent",
    "gdp_growth_rates": "US.GDPGR",
    "gdp_total_and_per_capita": "US.GDPTotal",
    "gender_domestic_value_added": "US.Gender_DomesticValueAdded",
    "gender_tradable_industries": "US.Gender_TradableIndustries",
    "gni_total_and_per_capita": "US.GNI",
    "goods_and_services_balance_bpm6": "US.GoodsAndServBalanceBpm6",
    "goods_and_services_bpm6": "US.GoodsAndServicesBpm6",
    "government_expenditures": "US.GovExpenditures",
    "hidden_plastics_trade": "US.HiddenPlasticsTradebyPartner",
    "ict_goods_share": "US.IctGoodsShare",
    "ict_goods_value": "US.IctGoodsValue",
    "ict_production_sector": "US.IctProductionSector",
    "ict_use_by_activity": "US.IctUseEconActivity",
    "ict_use_by_activity_isic4": "US.IctUseEconActivity_Isic4",
    "ict_use_by_enterprise_size": "US.IctUseEnterprSize",
    "ict_use_by_location": "US.IctUseLocation",
    "import_tariff_rates": "US.Tariff",
    "inclusive_growth": "US.InclusiveGrowth",
    "liner_shipping_bilateral_connectivity": "US.LSBCI",
    "liner_shipping_connectivity_annual": "US.LSCI",
    "liner_shipping_connectivity_monthly": "US.LSCI_M",
    "merchandise_concentration_indices": "US.ConcentDiversIndices",
    "merchandise_structural_indices": "US.ConcentStructIndices",
    "merchandise_terms_of_trade": "US.TermsOfTrade",
    "merchandise_theil_indices": "US.MerchTheilIndices",
    "merchandise_total_trade": "US.TradeMerchTotal",
    "merchandise_trade_balance": "US.TradeMerchBalance",
    "merchandise_trade_growth_rates": "US.TradeMerchGR",
    "merchandise_volume_quarterly": "US.MerchVolumeQuarterly",
    "merchant_fleet": "US.MerchantFleet",
    "non_plastic_substitutes_trade": "US.NonPlasticSubstsTradeByPartner",
    "ocean_exports_per_capita_individual": "US.OceanExportsPerCapitaIndividualEconomies",
    "ocean_exports_per_capita_regional": "US.OceanExportsPerCapitaRegionalAggregates",
    "ocean_rca_individual": "US.OceanRCAIndividualEconomies",
    "ocean_rca_regional": "US.OceanRCARegionalAggregates",
    "ocean_services": "US.OceanServices",
    "ocean_theil_indices": "US.OceanTheilIndicesIndividualEconomies",
    "ocean_trade_individual": "US.OceanTradeIndividualEconomies",
    "ocean_trade_regional": "US.OceanTradeRegionalAggregates",
    "personal_remittances": "US.Remittances",
    "plastics_trade_by_partner": "US.PlasticsTradebyPartner",
    "population_dependency_ratios": "US.PopDependency",
    "population_growth_rates": "US.PopGR",
    "population_structure_by_age_gender": "US.PopAgeStruct",
    "population_total_and_urban": "US.PopTotal",
    "port_calls": "US.PortCalls",
    "port_calls_arrivals": "US.PortCallsArrivals",
    "port_calls_arrivals_ships": "US.PortCallsArrivals_S",
    "port_calls_ships": "US.PortCalls_S",
    "port_liner_shipping_connectivity": "US.PLSCI",
    "productive_capacities_index": "US.PCI",
    "seaborne_trade": "US.SeaborneTrade",
    "seafarers": "US.Seafarers",
    "services_by_category": "US.TradeServCatByPartner",
    "services_trade_ict": "US.TradeServICT",
    "services_trade_quarterly": "US.TotAndComServicesQuarterly",
    "ship_building": "US.ShipBuilding",
    "ship_scrapping": "US.ShipScrapping",
    "trade_facilitation_index": "US.FTRI",
    "trade_openness_bpm6": "US.GoodsAndServTradeOpennessBpm6",
    "transport_costs": "US.TransportCosts",
    "unit_value_indices_annual": "US.UCPI_A",
    "unit_value_indices_monthly": "US.UCPI_M",
    "vessel_value_by_ownership": "US.VesselValueByOwnership",
    "vessel_value_by_registration": "US.VesselValueByRegistration",
}

MAX_FILE_SIZE = 200 * 1024 * 1024  # 200MB limit


def download_dataset(report_name: str) -> pa.Table:
    """Download a dataset from UNCTAD API.

    Args:
        report_name: UNCTAD report name (e.g., 'US.TradeMerchTotal')

    Returns:
        PyArrow table with the dataset
    """
    # Get file metadata
    metadata_url = f"https://unctadstat-api.unctad.org/api/reportMetadata/{report_name}/bulkfiles/en"
    metadata_resp = get(metadata_url)
    file_metadata = metadata_resp.json()[0]
    file_id = file_metadata["fileId"]
    file_size = file_metadata["fileSize"]

    if file_size > MAX_FILE_SIZE:
        raise ValueError(
            f"Dataset {report_name} size ({file_size / 1024 / 1024:.1f}MB) "
            f"exceeds maximum allowed size of {MAX_FILE_SIZE / 1024 / 1024:.0f}MB"
        )

    # Download the 7z archive
    download_url = f"https://unctadstat-api.unctad.org/api/reportMetadata/{report_name}/bulkfile/{file_id}/en"
    download_resp = get(download_url)
    archive_data = io.BytesIO(download_resp.content)

    # Extract CSV from archive to temp directory
    with tempfile.TemporaryDirectory() as tmpdir:
        with py7zr.SevenZipFile(archive_data, mode="r") as archive:
            all_files = archive.getnames()
            csv_files = [name for name in all_files if name.endswith(".csv")]

            if len(csv_files) != 1:
                raise ValueError(f"Expected 1 CSV file, found {len(csv_files)} in {report_name}")

            archive.extractall(path=tmpdir)

        # Parse CSV to PyArrow table
        csv_path = os.path.join(tmpdir, csv_files[0])
        table = pac.read_csv(csv_path)
        # Normalize column names: lowercase and underscores
        new_column_names = [col.lower().replace(" ", "_") for col in table.column_names]
        table = table.rename_columns(new_column_names)

    return table


def run():
    """Download all UNCTAD datasets."""
    print(f"Fetching {len(DATASET_MAPPING)} datasets from UNCTAD...")

    success = 0
    failed = []

    for asset_name, report_name in DATASET_MAPPING.items():
        print(f"  Downloading {asset_name} ({report_name})...")
        try:
            table = download_dataset(report_name)
            save_raw_parquet(asset_name, table)
            print(f"    ✓ {len(table)} rows")
            success += 1
        except Exception as e:
            print(f"    ✗ Error: {e}")
            failed.append((asset_name, str(e)))

    print(f"\nCompleted: {success}/{len(DATASET_MAPPING)} datasets")
    if failed:
        print(f"Failed ({len(failed)}):")
        for name, err in failed:
            print(f"  - {name}: {err}")
