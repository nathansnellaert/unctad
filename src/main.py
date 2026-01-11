from subsets_utils import DAG, validate_environment
from nodes import ingest
from nodes import associated_plastics_trade
from nodes import biotrade_concentration
from nodes import biotrade_gdp_share
from nodes import biotrade_market_indices
from nodes import biotrade_merchandise
from nodes import biotrade_rca
from nodes import biotrade_share
from nodes import commodity_price_indices_annual
from nodes import commodity_price_indices_monthly
from nodes import commodity_prices_annual
from nodes import commodity_prices_monthly
from nodes import consumer_price_index_annual
from nodes import container_port_throughput
from nodes import creative_goods_growth_rates
from nodes import creative_goods_index
from nodes import creative_goods_value
from nodes import creative_services_group_e
from nodes import creative_services_individual
from nodes import current_account_balance
from nodes import digitally_deliverable_services
from nodes import exchange_rates
from nodes import fdi_flows_and_stock
from nodes import fleet_beneficial_owners
from nodes import food_trade_by_processing
from nodes import food_trade_processing_by_category
from nodes import food_trade_rca_by_category
from nodes import food_trade_rca_by_processing
from nodes import gdp_by_expenditure_and_activity
from nodes import gdp_growth_rates
from nodes import gdp_total_and_per_capita
from nodes import gender_domestic_value_added
from nodes import gender_tradable_industries
from nodes import gni_total_and_per_capita
from nodes import goods_and_services_balance_bpm6
from nodes import goods_and_services_bpm6
from nodes import government_expenditures
from nodes import hidden_plastics_trade
from nodes import ict_goods_share
from nodes import ict_goods_value
from nodes import ict_production_sector
from nodes import ict_use_by_activity
from nodes import ict_use_by_activity_isic4
from nodes import ict_use_by_enterprise_size
from nodes import ict_use_by_location
from nodes import import_tariff_rates
from nodes import inclusive_growth
from nodes import liner_shipping_bilateral_connectivity
from nodes import liner_shipping_connectivity_annual
from nodes import liner_shipping_connectivity_monthly
from nodes import merchandise_concentration_indices
from nodes import merchandise_structural_indices
from nodes import merchandise_terms_of_trade
from nodes import merchandise_theil_indices
from nodes import merchandise_total_trade
from nodes import merchandise_trade_balance
from nodes import merchandise_trade_growth_rates
from nodes import merchandise_volume_quarterly
from nodes import merchant_fleet
from nodes import non_plastic_substitutes_trade
from nodes import ocean_exports_per_capita_individual
from nodes import ocean_exports_per_capita_regional
from nodes import ocean_rca_individual
from nodes import ocean_rca_regional
from nodes import ocean_services
from nodes import ocean_theil_indices
from nodes import ocean_trade_individual
from nodes import ocean_trade_regional
from nodes import personal_remittances
from nodes import plastics_trade_by_partner
from nodes import population_dependency_ratios
from nodes import population_growth_rates
from nodes import population_structure_by_age_gender
from nodes import population_total_and_urban
from nodes import port_calls
from nodes import port_calls_arrivals
from nodes import port_calls_arrivals_ships
from nodes import port_calls_ships
from nodes import port_liner_shipping_connectivity
from nodes import productive_capacities_index
from nodes import seaborne_trade
from nodes import seafarers
from nodes import services_by_category
from nodes import services_trade_ict
from nodes import services_trade_quarterly
from nodes import ship_building
from nodes import ship_scrapping
from nodes import trade_facilitation_index
from nodes import trade_openness_bpm6
from nodes import transport_costs
from nodes import unit_value_indices_annual
from nodes import unit_value_indices_monthly
from nodes import vessel_value_by_ownership
from nodes import vessel_value_by_registration


workflow = DAG({
    ingest.run: [],
    associated_plastics_trade.run: [ingest.run],
    biotrade_concentration.run: [ingest.run],
    biotrade_gdp_share.run: [ingest.run],
    biotrade_market_indices.run: [ingest.run],
    biotrade_merchandise.run: [ingest.run],
    biotrade_rca.run: [ingest.run],
    biotrade_share.run: [ingest.run],
    commodity_price_indices_annual.run: [ingest.run],
    commodity_price_indices_monthly.run: [ingest.run],
    commodity_prices_annual.run: [ingest.run],
    commodity_prices_monthly.run: [ingest.run],
    consumer_price_index_annual.run: [ingest.run],
    container_port_throughput.run: [ingest.run],
    creative_goods_growth_rates.run: [ingest.run],
    creative_goods_index.run: [ingest.run],
    creative_goods_value.run: [ingest.run],
    creative_services_group_e.run: [ingest.run],
    creative_services_individual.run: [ingest.run],
    current_account_balance.run: [ingest.run],
    digitally_deliverable_services.run: [ingest.run],
    exchange_rates.run: [ingest.run],
    fdi_flows_and_stock.run: [ingest.run],
    fleet_beneficial_owners.run: [ingest.run],
    food_trade_by_processing.run: [ingest.run],
    food_trade_processing_by_category.run: [ingest.run],
    food_trade_rca_by_category.run: [ingest.run],
    food_trade_rca_by_processing.run: [ingest.run],
    gdp_by_expenditure_and_activity.run: [ingest.run],
    gdp_growth_rates.run: [ingest.run],
    gdp_total_and_per_capita.run: [ingest.run],
    gender_domestic_value_added.run: [ingest.run],
    gender_tradable_industries.run: [ingest.run],
    gni_total_and_per_capita.run: [ingest.run],
    goods_and_services_balance_bpm6.run: [ingest.run],
    goods_and_services_bpm6.run: [ingest.run],
    government_expenditures.run: [ingest.run],
    hidden_plastics_trade.run: [ingest.run],
    ict_goods_share.run: [ingest.run],
    ict_goods_value.run: [ingest.run],
    ict_production_sector.run: [ingest.run],
    ict_use_by_activity.run: [ingest.run],
    ict_use_by_activity_isic4.run: [ingest.run],
    ict_use_by_enterprise_size.run: [ingest.run],
    ict_use_by_location.run: [ingest.run],
    import_tariff_rates.run: [ingest.run],
    inclusive_growth.run: [ingest.run],
    liner_shipping_bilateral_connectivity.run: [ingest.run],
    liner_shipping_connectivity_annual.run: [ingest.run],
    liner_shipping_connectivity_monthly.run: [ingest.run],
    merchandise_concentration_indices.run: [ingest.run],
    merchandise_structural_indices.run: [ingest.run],
    merchandise_terms_of_trade.run: [ingest.run],
    merchandise_theil_indices.run: [ingest.run],
    merchandise_total_trade.run: [ingest.run],
    merchandise_trade_balance.run: [ingest.run],
    merchandise_trade_growth_rates.run: [ingest.run],
    merchandise_volume_quarterly.run: [ingest.run],
    merchant_fleet.run: [ingest.run],
    non_plastic_substitutes_trade.run: [ingest.run],
    ocean_exports_per_capita_individual.run: [ingest.run],
    ocean_exports_per_capita_regional.run: [ingest.run],
    ocean_rca_individual.run: [ingest.run],
    ocean_rca_regional.run: [ingest.run],
    ocean_services.run: [ingest.run],
    ocean_theil_indices.run: [ingest.run],
    ocean_trade_individual.run: [ingest.run],
    ocean_trade_regional.run: [ingest.run],
    personal_remittances.run: [ingest.run],
    plastics_trade_by_partner.run: [ingest.run],
    population_dependency_ratios.run: [ingest.run],
    population_growth_rates.run: [ingest.run],
    population_structure_by_age_gender.run: [ingest.run],
    population_total_and_urban.run: [ingest.run],
    port_calls.run: [ingest.run],
    port_calls_arrivals.run: [ingest.run],
    port_calls_arrivals_ships.run: [ingest.run],
    port_calls_ships.run: [ingest.run],
    port_liner_shipping_connectivity.run: [ingest.run],
    productive_capacities_index.run: [ingest.run],
    seaborne_trade.run: [ingest.run],
    seafarers.run: [ingest.run],
    services_by_category.run: [ingest.run],
    services_trade_ict.run: [ingest.run],
    services_trade_quarterly.run: [ingest.run],
    ship_building.run: [ingest.run],
    ship_scrapping.run: [ingest.run],
    trade_facilitation_index.run: [ingest.run],
    trade_openness_bpm6.run: [ingest.run],
    transport_costs.run: [ingest.run],
    unit_value_indices_annual.run: [ingest.run],
    unit_value_indices_monthly.run: [ingest.run],
    vessel_value_by_ownership.run: [ingest.run],
    vessel_value_by_registration.run: [ingest.run],
})


def main():
    validate_environment()
    workflow.run()


if __name__ == "__main__":
    main()
