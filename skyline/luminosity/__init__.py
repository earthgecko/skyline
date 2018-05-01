"""
Luminosity

- Determine correlations
- Insert in correlations table (slow stream of inserts, not gluts, queue them)
    - anomaly_id, mertic_id, coefficient, shift, shifted_coefficient
    - a table per metric a la z_ts or;
    - a table per month and a daily aggregated table?
- In Ionosphere:
    - Display Luminosity sorted_correlated_metrics in a block with a link to the
      anomaly page
- In Panorama:
    - Display a Luminosity link the anomaly page

"""
