from _helper import check_urls

if __name__ == "__main__":
    parameters = {"year":2013, "bra_id":'am', "cbo_id":'1', "month": 1, "hs_id": '01', "cnae_id": 27325, "wld_id": 'sa' }

    endpoints = [
        "/secex/<year>/<bra_id>/show/all",
        "/secex/<year>/<bra_id>/show/<wld_id>",
        "/secex/<year>/<bra_id>/all/show",
        "/secex/<year>/<bra_id>/<hs_id>/show",
        "/secex/<year>/<bra_id>.show.8/all/all",
        "/secex/<year>/<bra_id>.show.8/<hs_id>/all",
        "/secex/<year>/<bra_id>.show.8/all/<wld_id>",
        "/secex/<year>/<bra_id>.show.8/<hs_id>/<wld_id>",
        "/secex/<year>/<bra_id>_<bra_id>/all/show",
        "/secex/<year>/<bra_id>_<bra_id>/<hs_id>/show",
        "/secex/<year>/<bra_id>_<bra_id>/show/all",
        "/secex/<year>/<bra_id>_<bra_id>/show/<wld_id>",
        "/secex/<year>/<bra_id>/show/all",
        "/secex/<year>/<bra_id>/show/all",
    ]


    check_urls(endpoints, parameters)
