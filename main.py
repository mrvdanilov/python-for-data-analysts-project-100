from charts_project.tools import *
import os
from dotenv import load_dotenv
import requests
from pathlib import Path


load_dotenv()

DATE_BEGIN = os.getenv('DATE_BEGIN')
DATE_END = os.getenv('DATE_END')
API_URL = os.getenv('API_URL')


def get_data_path(file_name):
    current_dir = Path(__file__).absolute().parent
    return current_dir / 'data' / file_name


def main():
    p = Path.cwd()
    d = p / 'charts'
    d.mkdir(exist_ok=True)
    date_start = DATE_BEGIN
    date_end = DATE_END
    date_group = 'day'
    run_and_analyze(date_start, date_end, date_group)


def run_and_analyze(date_start, date_end, date_group):
    resp = requests.get(f'{API_URL}/registrations', params={'begin': date_start, 'end': date_end})
    regs = pd.DataFrame(resp.json())

    resp = requests.get(f'{API_URL}/visits', params={'begin': date_start, 'end': date_end})
    visits = pd.DataFrame(resp.json())

    regs.rename(columns={'datetime': 'registration_dt'}, inplace=True)
    visits.rename(columns={'visit_id': 'anonymous_id', 'datetime': 'visit_dt'}, inplace=True)

    ads_column_names = ['date', 'source', 'medium', 'campaign', 'cost']
    ads = pd.read_csv(get_data_path('ads.csv'), names=ads_column_names, header=None)

    clean_regs = clean_registration_data(regs)
    aggregated_df_with_regtype, aggregated_df_without_regtype = filter_and_aggregate_registration_data(clean_regs,
                                                                                                       date_start,
                                                                                                       date_end,
                                                                                                       date_group)
    if not aggregated_df_with_regtype.empty:
        visualize_aggregated_registration_data(aggregated_df_with_regtype)

    clean_visits = clean_visit_data(visits)
    aggregated_visits = filter_and_aggregate_visit_data(clean_visits, date_start, date_end, date_group)
    if not aggregated_visits.empty:
        visualize_aggregated_visits_data(aggregated_visits)

    if not (aggregated_df_without_regtype.empty or aggregated_visits.empty):
        conversion = merge_dataframes_and_calculate_conversion(aggregated_visits, aggregated_df_without_regtype)
        plot_conversion_graphs(conversion)

    ads_data_cleaned = clean_ads_data(ads)
    aggregated_ads_data, df_ads_aggregated_with_ads = filter_and_aggregate_ads_data(ads_data_cleaned, date_start, date_end, date_group)
    if not aggregated_ads_data.empty:
        visualize_aggregated_ads_data(aggregated_ads_data, date_group)

    df_ads_periods = get_continuous_campaign_periods(df_ads_aggregated_with_ads, date_group)
    visualize_combined_data(aggregated_visits, aggregated_df_without_regtype, df_ads_periods)


if __name__ == '__main__':
    main()
