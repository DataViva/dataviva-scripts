import requests
import json

class NoDataError(Exception): pass
class BadHTTPStatusError(Exception): pass

def replace_params(url, params):
    for param, value in params.items():
        formatted_param = "<%s>" % (param)
        url = url.replace(formatted_param, str(value))
    return url

def get_data(full_url):
    req = requests.get(full_url)
    if req.status_code != 200:
        raise BadHTTPStatusError("%s returned a status code of %s" % (url, req.status_code))
    else:
        json_data = req.json()
        if not "data" in json_data or not json_data["data"]:
            raise NoDataError("No data in response from %s" % url)

def check_urls(urls, params, base_url='http://127.0.0.1:5000'):
    failures = 0

    for url in urls:
        url_with_params = replace_params(url, params)

        try:
            print "Checking", url_with_params, "..."
            get_data(base_url + url_with_params)
            print "OK"
        except (NoDataError, BadHTTPStatusError) as e:
            print "** API Endpoint Failure occured with endpoint", url, e
            failures += 1

    print "\nSummary:"
    if not failures:
        print "No failures!"
    else:
        print "%s failures occured" % (failures)
        print "Percent failed: %.2f%%" % (100 * failures / float(len(urls)))



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
         '/secex/all/<month>/<bra_id>/all/all/',
         '/secex/<year>/<month>/<bra_id>/all/all/',
         '/secex/all/<month>/all/<hs_id>/all/',
         '/secex/<year>/<month>/all/<hs_id>/all/',
         '/secex/all/<month>/all/all/<wld_id>/',
         '/secex/<year>/<month>/all/all/<wld_id>/',
         '/secex/all/<month>/<bra_id>/all/<wld_id>/',
         '/secex/<year>/<month>/<bra_id>/all/<wld_id>/',
         '/secex/all/<month>/<bra_id>/<hs_id>/all/',
         '/secex/<year>/<month>/<bra_id>/<hs_id>/all/',
         '/secex/all/<month>/all/<hs_id>/<wld_id>/',
         '/secex/<year>/<month>/all/<hs_id>/<wld_id>/',
         '/secex/all/<month>/<bra_id>/<hs_id>/<wld_id>/',
         '/secex/<year>/<month>/rj/01/af/'
    ]


    check_urls(endpoints, parameters)
