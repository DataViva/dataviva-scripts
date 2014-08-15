from _helper import check_urls

if __name__ == "__main__":
    parameters = {"year":2002, "bra_id":'am', "cbo_id":'1', "month": 1, "hs_id": '01', "cnae_id": 27325, "wld_id": 'sa' }

    endpoints = [
         '/rais/all/<bra_id>/all/all/',
         '/rais/<year>/<bra_id>/all/all/',
         '/rais/all/all/<cnae_id>/all/',
         '/rais/<year>/all/<cnae_id>/all/',
         '/rais/all/all/all/<cbo_id>/',
         '/rais/<year>/all/all/<cbo_id>/',
         '/rais/all/<bra_id>/<cnae_id>/all/',
         '/rais/<year>/<bra_id>/<cnae_id>/all/',
         '/rais/all/<bra_id>/all/<cbo_id>/',
         '/rais/<year>/<bra_id>/all/<cbo_id>/',
         '/rais/all/all/<cnae_id>/<cbo_id>/',
         '/rais/<year>/all/<cnae_id>/<cbo_id>/',
         '/rais/all/<bra_id>/<cnae_id>/<cbo_id>/',
         '/rais/<year>/<bra_id>/<cnae_id>/<cbo_id>/',
         '/secex/all/<bra_id>/all/all/',
         '/secex/<year>/<bra_id>/all/all/',
         '/secex/all/all/<hs_id>/all/',
         '/secex/<year>/all/<hs_id>/all/',
         '/secex/all/all/all/<wld_id>/',
         '/secex/<year>/all/all/<wld_id>/',
         '/secex/all/<bra_id>/all/<wld_id>/',
         '/secex/<year>/<bra_id>/all/<wld_id>/',
         '/secex/all/<bra_id>/<hs_id>/all/',
         '/secex/<year>/<bra_id>/<hs_id>/all/',
         '/secex/all/all/<hs_id>/<wld_id>/',
         '/secex/<year>/all/<hs_id>/<wld_id>/',
         '/secex/all/<bra_id>/<hs_id>/<wld_id>/',
         '/secex/<year>/rj/01/af/'
    ]


    check_urls(endpoints, parameters)
