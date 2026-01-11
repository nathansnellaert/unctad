"""UNCTAD connector - Generated from catalog/status.json.

Do not edit directly. Run: python catalog/compile.py
"""
from subsets_utils import DAG, validate_environment
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
    associated_plastics_trade.download: [],
    biotrade_concentration.download: [],
    biotrade_gdp_share.download: [],
    biotrade_market_indices.download: [],
    biotrade_merchandise.download: [],
    biotrade_rca.download: [],
    biotrade_share.download: [],
    commodity_price_indices_annual.download: [],
    commodity_price_indices_monthly.download: [],
    commodity_prices_annual.download: [],
    commodity_prices_monthly.download: [],
    consumer_price_index_annual.download: [],
    container_port_throughput.download: [],
    creative_goods_growth_rates.download: [],
    creative_goods_index.download: [],
    creative_goods_value.download: [],
    creative_services_group_e.download: [],
    creative_services_individual.download: [],
    current_account_balance.download: [],
    digitally_deliverable_services.download: [],
    exchange_rates.download: [],
    fdi_flows_and_stock.download: [],
    fleet_beneficial_owners.download: [],
    food_trade_by_processing.download: [],
    food_trade_processing_by_category.download: [],
    food_trade_rca_by_category.download: [],
    food_trade_rca_by_processing.download: [],
    gdp_by_expenditure_and_activity.download: [],
    gdp_growth_rates.download: [],
    gdp_total_and_per_capita.download: [],
    gender_domestic_value_added.download: [],
    gender_tradable_industries.download: [],
    gni_total_and_per_capita.download: [],
    goods_and_services_balance_bpm6.download: [],
    goods_and_services_bpm6.download: [],
    government_expenditures.download: [],
    hidden_plastics_trade.download: [],
    ict_goods_share.download: [],
    ict_goods_value.download: [],
    ict_production_sector.download: [],
    ict_use_by_activity.download: [],
    ict_use_by_activity_isic4.download: [],
    ict_use_by_enterprise_size.download: [],
    ict_use_by_location.download: [],
    import_tariff_rates.download: [],
    inclusive_growth.download: [],
    liner_shipping_bilateral_connectivity.download: [],
    liner_shipping_connectivity_annual.download: [],
    liner_shipping_connectivity_monthly.download: [],
    merchandise_concentration_indices.download: [],
    merchandise_structural_indices.download: [],
    merchandise_terms_of_trade.download: [],
    merchandise_theil_indices.download: [],
    merchandise_total_trade.download: [],
    merchandise_trade_balance.download: [],
    merchandise_trade_growth_rates.download: [],
    merchandise_volume_quarterly.download: [],
    merchant_fleet.download: [],
    non_plastic_substitutes_trade.download: [],
    ocean_exports_per_capita_individual.download: [],
    ocean_exports_per_capita_regional.download: [],
    ocean_rca_individual.download: [],
    ocean_rca_regional.download: [],
    ocean_services.download: [],
    ocean_theil_indices.download: [],
    ocean_trade_individual.download: [],
    ocean_trade_regional.download: [],
    personal_remittances.download: [],
    plastics_trade_by_partner.download: [],
    population_dependency_ratios.download: [],
    population_growth_rates.download: [],
    population_structure_by_age_gender.download: [],
    population_total_and_urban.download: [],
    port_calls.download: [],
    port_calls_arrivals.download: [],
    port_calls_arrivals_ships.download: [],
    port_calls_ships.download: [],
    port_liner_shipping_connectivity.download: [],
    productive_capacities_index.download: [],
    seaborne_trade.download: [],
    seafarers.download: [],
    services_by_category.download: [],
    services_trade_ict.download: [],
    services_trade_quarterly.download: [],
    ship_building.download: [],
    ship_scrapping.download: [],
    trade_facilitation_index.download: [],
    trade_openness_bpm6.download: [],
    transport_costs.download: [],
    unit_value_indices_annual.download: [],
    unit_value_indices_monthly.download: [],
    vessel_value_by_ownership.download: [],
    vessel_value_by_registration.download: [],

    # associated_plastics_trade.transform: [associated_plastics_trade.download],
    # biotrade_concentration.transform: [biotrade_concentration.download],
    # biotrade_gdp_share.transform: [biotrade_gdp_share.download],
    # biotrade_market_indices.transform: [biotrade_market_indices.download],
    # biotrade_merchandise.transform: [biotrade_merchandise.download],
    # biotrade_rca.transform: [biotrade_rca.download],
    # biotrade_share.transform: [biotrade_share.download],
    # commodity_price_indices_annual.transform: [commodity_price_indices_annual.download],
    # commodity_price_indices_monthly.transform: [commodity_price_indices_monthly.download],
    # commodity_prices_annual.transform: [commodity_prices_annual.download],
    # commodity_prices_monthly.transform: [commodity_prices_monthly.download],
    # consumer_price_index_annual.transform: [consumer_price_index_annual.download],
    # container_port_throughput.transform: [container_port_throughput.download],
    # creative_goods_growth_rates.transform: [creative_goods_growth_rates.download],
    # creative_goods_index.transform: [creative_goods_index.download],
    # creative_goods_value.transform: [creative_goods_value.download],
    # creative_services_group_e.transform: [creative_services_group_e.download],
    # creative_services_individual.transform: [creative_services_individual.download],
    # current_account_balance.transform: [current_account_balance.download],
    # digitally_deliverable_services.transform: [digitally_deliverable_services.download],
    # exchange_rates.transform: [exchange_rates.download],
    # fdi_flows_and_stock.transform: [fdi_flows_and_stock.download],
    # fleet_beneficial_owners.transform: [fleet_beneficial_owners.download],
    # food_trade_by_processing.transform: [food_trade_by_processing.download],
    # food_trade_processing_by_category.transform: [food_trade_processing_by_category.download],
    # food_trade_rca_by_category.transform: [food_trade_rca_by_category.download],
    # food_trade_rca_by_processing.transform: [food_trade_rca_by_processing.download],
    # gdp_by_expenditure_and_activity.transform: [gdp_by_expenditure_and_activity.download],
    # gdp_growth_rates.transform: [gdp_growth_rates.download],
    # gdp_total_and_per_capita.transform: [gdp_total_and_per_capita.download],
    # gender_domestic_value_added.transform: [gender_domestic_value_added.download],
    # gender_tradable_industries.transform: [gender_tradable_industries.download],
    # gni_total_and_per_capita.transform: [gni_total_and_per_capita.download],
    # goods_and_services_balance_bpm6.transform: [goods_and_services_balance_bpm6.download],
    # goods_and_services_bpm6.transform: [goods_and_services_bpm6.download],
    # government_expenditures.transform: [government_expenditures.download],
    # hidden_plastics_trade.transform: [hidden_plastics_trade.download],
    # ict_goods_share.transform: [ict_goods_share.download],
    # ict_goods_value.transform: [ict_goods_value.download],
    # ict_production_sector.transform: [ict_production_sector.download],
    # ict_use_by_activity.transform: [ict_use_by_activity.download],
    # ict_use_by_activity_isic4.transform: [ict_use_by_activity_isic4.download],
    # ict_use_by_enterprise_size.transform: [ict_use_by_enterprise_size.download],
    # ict_use_by_location.transform: [ict_use_by_location.download],
    # import_tariff_rates.transform: [import_tariff_rates.download],
    # inclusive_growth.transform: [inclusive_growth.download],
    # liner_shipping_bilateral_connectivity.transform: [liner_shipping_bilateral_connectivity.download],
    # liner_shipping_connectivity_annual.transform: [liner_shipping_connectivity_annual.download],
    # liner_shipping_connectivity_monthly.transform: [liner_shipping_connectivity_monthly.download],
    # merchandise_concentration_indices.transform: [merchandise_concentration_indices.download],
    # merchandise_structural_indices.transform: [merchandise_structural_indices.download],
    # merchandise_terms_of_trade.transform: [merchandise_terms_of_trade.download],
    # merchandise_theil_indices.transform: [merchandise_theil_indices.download],
    # merchandise_total_trade.transform: [merchandise_total_trade.download],
    # merchandise_trade_balance.transform: [merchandise_trade_balance.download],
    # merchandise_trade_growth_rates.transform: [merchandise_trade_growth_rates.download],
    # merchandise_volume_quarterly.transform: [merchandise_volume_quarterly.download],
    # merchant_fleet.transform: [merchant_fleet.download],
    # non_plastic_substitutes_trade.transform: [non_plastic_substitutes_trade.download],
    # ocean_exports_per_capita_individual.transform: [ocean_exports_per_capita_individual.download],
    # ocean_exports_per_capita_regional.transform: [ocean_exports_per_capita_regional.download],
    # ocean_rca_individual.transform: [ocean_rca_individual.download],
    # ocean_rca_regional.transform: [ocean_rca_regional.download],
    # ocean_services.transform: [ocean_services.download],
    # ocean_theil_indices.transform: [ocean_theil_indices.download],
    # ocean_trade_individual.transform: [ocean_trade_individual.download],
    # ocean_trade_regional.transform: [ocean_trade_regional.download],
    # personal_remittances.transform: [personal_remittances.download],
    # plastics_trade_by_partner.transform: [plastics_trade_by_partner.download],
    # population_dependency_ratios.transform: [population_dependency_ratios.download],
    # population_growth_rates.transform: [population_growth_rates.download],
    # population_structure_by_age_gender.transform: [population_structure_by_age_gender.download],
    # population_total_and_urban.transform: [population_total_and_urban.download],
    # port_calls.transform: [port_calls.download],
    # port_calls_arrivals.transform: [port_calls_arrivals.download],
    # port_calls_arrivals_ships.transform: [port_calls_arrivals_ships.download],
    # port_calls_ships.transform: [port_calls_ships.download],
    # port_liner_shipping_connectivity.transform: [port_liner_shipping_connectivity.download],
    # productive_capacities_index.transform: [productive_capacities_index.download],
    # seaborne_trade.transform: [seaborne_trade.download],
    # seafarers.transform: [seafarers.download],
    # services_by_category.transform: [services_by_category.download],
    # services_trade_ict.transform: [services_trade_ict.download],
    # services_trade_quarterly.transform: [services_trade_quarterly.download],
    # ship_building.transform: [ship_building.download],
    # ship_scrapping.transform: [ship_scrapping.download],
    # trade_facilitation_index.transform: [trade_facilitation_index.download],
    # trade_openness_bpm6.transform: [trade_openness_bpm6.download],
    # transport_costs.transform: [transport_costs.download],
    # unit_value_indices_annual.transform: [unit_value_indices_annual.download],
    # unit_value_indices_monthly.transform: [unit_value_indices_monthly.download],
    # vessel_value_by_ownership.transform: [vessel_value_by_ownership.download],
    # vessel_value_by_registration.transform: [vessel_value_by_registration.download],
})


def main():
    validate_environment()
    workflow.run()


if __name__ == "__main__":
    main()
