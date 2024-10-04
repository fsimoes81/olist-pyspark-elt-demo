from typing import List, Callable
import papermill as pm
import os
import sys
import argparse

def run_layer(nbs: List[str], output_path: str, base_path: str):
    os.makedirs(output_path, exist_ok=True)
    for nb in nbs:
        nb_name = os.path.basename(nb)
        output_nb = os.path.join(output_path, f"output_{nb_name}")
        
        pm.execute_notebook(
            nb,
            output_nb,
            parameters={"base_path": base_path}
        )

def get_notebooks(base_path: str, notebook_names: List[str]) -> List[str]:
    return [os.path.join(base_path, nb) for nb in notebook_names]

def get_bronze_layer_notebooks(bronze_nbs_path: str) -> List[str]:
    notebook_names = [
        'stg_brz_customer.ipynb',
        'stg_brz_geolocation.ipynb',
        'stg_brz_order_items.ipynb',
        'stg_brz_order_payments.ipynb',
        'stg_brz_order_reviews.ipynb',
        'stg_brz_orders.ipynb',
        'stg_brz_product_category_translation.ipynb',
        'stg_brz_products.ipynb',
        'stg_brz_sellers.ipynb'
    ]
    return get_notebooks(bronze_nbs_path, notebook_names)

def get_silver_layer_notebooks(silver_nbs_path: str) -> List[str]:
    mtd_notebooks = [
        'mtd/brz_slv_mtd_customer.ipynb',
        'mtd/brz_slv_mtd_geolocation.ipynb',
        'mtd/brz_slv_mtd_products.ipynb',
        'mtd/brz_slv_mtd_sellers.ipynb',
    ]
    evt_notebooks = [
        'evt/brz_slv_evt_order_items.ipynb',
        'evt/brz_slv_evt_order_payments.ipynb',
        'evt/brz_slv_evt_order_reviews.ipynb',
        'evt/brz_slv_evt_orders.ipynb',
    ]
    return get_notebooks(silver_nbs_path, mtd_notebooks + evt_notebooks)

def get_gold_layer_notebooks(gold_nbs_path: str) -> List[str]:
    dim_notebooks = [
        'dim/slv_gld_dim_customer.ipynb',
        'dim/slv_gld_dim_geolocation.ipynb',
        'dim/slv_gld_dim_products.ipynb',
        'dim/slv_gld_dim_sellers.ipynb',
    ]
    fct_notebooks = [
        'fct/slv_gld_fct_order_items.ipynb',
        'fct/slv_gld_fct_order_payments.ipynb',
        'fct/slv_gld_fct_order_reviews.ipynb',
        'fct/slv_gld_fct_orders.ipynb',
    ]
    return get_notebooks(gold_nbs_path, dim_notebooks + fct_notebooks)

def process_layer(base_path: str, get_notebooks_func: Callable[[str], List[str]], data_base_path: str):
    notebooks = get_notebooks_func(base_path)
    output_path = os.path.join(base_path, 'temp')
    run_layer(notebooks, output_path, data_base_path)

def main(layers_to_run):
    notebook_base_dir = '../../notebooks' 
    data_base_dir = '../../data'  # Adjust this to your actual data directory

    # layers = {
    #     'brz': ('bronze', get_bronze_layer_notebooks, os.path.join(data_base_dir, 'bronze')),
    #     'slv': ('silver', get_silver_layer_notebooks, os.path.join(data_base_dir, 'silver')),
    #     'gld': ('gold', get_gold_layer_notebooks, os.path.join(data_base_dir, 'gold'))
    # }

    layers = {
        'brz': ('bronze', get_bronze_layer_notebooks, data_base_dir),
        'slv': ('silver', get_silver_layer_notebooks, data_base_dir),
        'gld': ('gold', get_gold_layer_notebooks, data_base_dir)
    }    

    for layer in layers_to_run:
        if layer in layers:
            folder, get_notebooks_func, data_base_path = layers[layer]
            print(f"Processing {layer.upper()} layer...")
            process_layer(os.path.join(notebook_base_dir, folder), get_notebooks_func, data_base_path)
        else:
            print(f"Unknown layer: {layer}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run data pipeline layers")
    parser.add_argument('layers', nargs='*', default=['brz', 'slv', 'gld'], 
                        help="Layers to run (brz, slv, gld). If not specified, all layers will be run.")
    args = parser.parse_args()

    main(args.layers)