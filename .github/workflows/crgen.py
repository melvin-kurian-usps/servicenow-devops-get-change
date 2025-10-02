import json
import ruamel.yaml
from os import environ as env
from os import path, walk, sep
from time import strftime, localtime, time, sleep
import argparse
import requests
import re, sys

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

parser = argparse.ArgumentParser()
parser.add_argument("--cr_json", help="Generate Initial CR JSON", action="store_true")
parser.add_argument("--cr_update", help="Generate CR JSON for Update (with callback URL and PR/CR number)", action="store_true")
parser.add_argument("--deploy", help="ArgoCD Sync. Monitor Sync Status.", action="store_true")
parser.add_argument("--imageupdate", help="Update image tags for deployment.", action="store_true")
parser.add_argument("--verbose", help="More/wordier output.", action="store_true")
parser.add_argument("--simple", help="Omit custom fields and stuff when creating the CR.", action="store_true")
parser.add_argument("--secretname", help="Just spit out name of argocd server.", action="store_true")
parser.add_argument("--merge", help="Merge currently active Pull Request", action="store_true")
args = parser.parse_args()

class pr_merger:
    def __init__(self):
        self.pr_number = env['PR_NUMBER']
        self.pr_url = f"https://api.github.com/repos/{env['REPO']}/pulls/{self.pr_number}"
        self.merge_url = f"{self.pr_url}/merge"
        self.github_headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Authorization": f"Bearer {env['GITHUB_TOKEN']}"
        }
        self.merge_data = {
            "commit_title": "ArgoCD Deploy",
            "commit_message": f"Merge Deploy PR for CR {env['CR_NUMBER']}"
        }
        return

    def check_mergeability(self):
        # There's a "mergeable" attribute for a PR that is lazily populated when you fetch the PR and then fetch it again a little bit later.
        r = requests.get( self.pr_url, headers = self.github_headers )
        print("Waiting five seconds for mergeability calculation.", file=sys.stderr)
        sleep(5)
        r = requests.get( self.pr_url, headers = self.github_headers )
        pr = r.json()

        isError = False
        if r.status_code > 299:
            print("Unable to query PR status.", r.text)
            isError = True
        elif pr['mergeable'] in [ False, None ]:
            print("Pull request", self.pr_number, "is not in a mergeable state.")
            isError = True
        elif pr['merged_at'] is not None:
            print("Pull request", self.pr_number, "was already merged.")
            isError = True
        else:
            print("PR is mergeable.", file=sys.stderr)

        return isError

    def merge(self):
        isError = False
        r = requests.put( self.merge_url, headers = self.github_headers, data = json.dumps(self.merge_data) )
        if r.status_code > 299:
            print(r.text, file=sys.stderr)
            isError = True
        else:
            print("Merge successful")
            isError = False
        return isError

    def check_and_merge(self):
        isError = False
        isError = self.check_mergeability()

        if not isError:
            isError = self.merge()
        return isError

class deployment_updater:
    REQ_VAR = [ "YAML_PROPERTY", "NEW_VALUE", "ENVIRONMENT", "SUBFOLDER_FILTER", "FILENAME_FILTER" ]
    yaml = ruamel.yaml.YAML()

    def __init__(self):
        for v in self.REQ_VAR:
            if v not in env.keys():
                raise Exception(f"Missing environment variable {v}")
        return

    def update_deployments(self):
        if args.verbose:
            for variable_name in self.REQ_VAR:
                print(f"{variable_name}: {env[variable_name]}")
            print("")

            for file in self.find_files():
                print("Updating", file)
                self.update_property(file)
                print("Update complete for", file)
                print("File Contents:")
                print(open(file,'r').read())
                print("")
            return

        else:
            for file in self.find_files():
                self.update_property(file)
        return

    def find_all_files(self):
        file_paths = []
        for root, _, files in walk('.'):
            for file in files:
                file_paths.append(path.join(root, file))
        return file_paths

    def set_property(self, proplist, the_dict, newvalue):
        if len(proplist) == 1:
            if proplist[0] in the_dict.keys():
                the_dict[proplist[0]] = newvalue
                return True
            else:
                return False
        else:
            return self.set_property(proplist[1:], the_dict[proplist[0]], newvalue)

    def follow_properties(self, proplist, the_dict):
        if len(proplist) == 1:
            try:
                if proplist[0] in the_dict.keys():
                    return True
                else:
                    return False
            except:
                return False

        try:
            if proplist[0] in the_dict.keys():
                return self.follow_properties(proplist[1:], the_dict[proplist[0]])
            else:
                return False
        except:
            return False

    def contains_property(self, file, file_property):
        try:
            with open(file, 'r') as f:
                data = f.read()
            data = self.yaml.load(data)
        except:
            return False
        properties_list = file_property.split('.')
        return self.follow_properties(properties_list, data)


    def update_property(self, file):
        update_properties = [ prop for prop in env['YAML_PROPERTY'].split(',') ]

        for propertystring in update_properties:
            if self.contains_property(file, propertystring):
                with open(file, 'r') as f:
                    data = f.read()
                data = self.yaml.load(data)
                if self.set_property(propertystring.split('.'), data, env['NEW_VALUE']):
                    with open(file, 'w') as f:
                        self.yaml.dump(data, f)
                else:
                    print("Unable to update property in", file)
            else:
                print("File", file, "does not contain property", propertystring)


    def find_files(self):
        file_paths = self.find_all_files()

        # filter yaml files only
        file_paths = [ file for file in file_paths if file.lower().endswith(".yml") or file.lower().endswith(".yaml") ]

        # optionally filter on FILENAME_FILTER
        if env['FILENAME_FILTER'] != "*":
            file_paths = [ file for file in file_paths if env['FILENAME_FILTER'].lower() in file.lower() ]

        # optionally filter on SUBFOLDER_FILTER
        if env['SUBFOLDER_FILTER'] != "*":
            file_paths = [ file for file in file_paths if env['SUBFOLDER_FILTER'].lower() in file.lower() ]

        # filter on files containing the property
        file_paths = [ file for file in file_paths if any([ self.contains_property(file, file_property) for file_property in env['YAML_PROPERTY'].split(',') ]) ]

        return file_paths


class templater:
    REQ_VAR = [ "ENVIRONMENT", "NEW_VALUE", "SUBFOLDER_FILTER", "FILENAME_FILTER", "PR_NUMBER", "CR_NUMBER", "REPO", "ACTOR", "CORRELATION_URL" ]
    VARMAP = None
    yaml = ruamel.yaml.YAML()

    tslc_activity_options = [
        "Application Configuration Change",
        "Bound app servers to VIP",
        "Code or Data Change",
        "Code Promotion",
        "Data extract",
        "Database Changes",
        "DB Configuration Change",
        "Decommission",
        "Emergency CI Change",
        "JCL Change",
        "Job Schedule Change",
        "Maintenance",
        "MSR promotion",
        "OS or DB SW Patch Change",
        "SW Code Change",
        "SW Upgrade Change"
    ]


    def __init__(self):
        for v in self.REQ_VAR:
            if v not in env.keys():
                raise Exception(f"Environment variable missing: '" + v + "' must be set.")
        self.VARMAP = dict()
        for v in self.REQ_VAR:
            self.VARMAP[v] = env[v]

        self.argocd_server = env['ARGOCD_SERVER'].format(env = env['ENVIRONMENT'].lower())
        if "https://" not in self.argocd_server:
            self.argocd_server = "https://" + self.argocd_server
        self.requesting_user = env['ACTOR'].replace("usps", "").replace("-", " ").strip().title()
        return

    def load_user_template(self):
        dir_path = path.dirname(path.realpath(__file__))

        cr_creation_path = f"{dir_path}{sep}..{sep}..{sep}cr_creation.yml"
        cr_creation_path = path.realpath(cr_creation_path)

        if not path.exists(cr_creation_path):
            exit(f"Missing {cr_creation_path}")

        with open(cr_creation_path) as f:
            data = self.yaml.load(f)

        required_template_values = [ "business_service", "cmdb_name", "request_group", "assignment_group" ]
        for varname in required_template_values:
            if varname not in data.keys():
                exit(f"{varname} is required but missing from {cr_creation_path}")

        for varname in data.keys():
            if "default_" in varname:
                print(f"Found optional variable {varname} - this will override a default value.")

        return data

    def generate_cr_template(self):
        data = self.load_user_template()

        five_minutes_from_now = int(time()) + 300
        three_days_from_now = int(time()) + (86400 * 3) + 3600
        seven_days_from_now = int(time()) + (86400 * 7) + 3600
        if "p" in env['ENVIRONMENT'].lower():
            start_date = strftime('%Y-%m-%d %H:%M:%S', localtime( three_days_from_now ) )
            end_date = strftime('%Y-%m-%d %H:%M:%S', localtime( seven_days_from_now ) )
        else:
            start_date = strftime('%Y-%m-%d %H:%M:%S', localtime( five_minutes_from_now ) )
            end_date = strftime('%Y-%m-%d %H:%M:%S', localtime( three_days_from_now ) )

        # Store these values in the VARMAP for variable substitution/formatting.
        self.VARMAP['start_date'] = start_date
        self.VARMAP['end_date'] = end_date
        self.VARMAP['requesting_user'] = self.requesting_user

        crtemplate = {
            "setCloseCode": False,
            "attributes": {
                "business_service": {
                    "name": data['business_service']
                },
                "cmdb_ci": {
                    "name": data['cmdb_name']
                },
                "requested_by": { "name": self.requesting_user },
                "u_requesting_group": data['request_group'],
                "u_environment": env['ENVIRONMENT'],
                "start_date": start_date,
                "end_date": end_date,
                "category": "DevOps",
                "u_subcategory": "Code Promotion",
                "u_subcategory_2": "Application Executable",
                "u_outage": "No Outage",
                "u_pci_inscope": data['pci_in_scope'],
                "u_tslc_project": data['u_tslc_project'],
                "assignment_group": { "name": data['assignment_group'] },
                "short_description": f"Update {data['cmdb_name']} in {env['ENVIRONMENT']} to {env['NEW_VALUE']}",
                "description": f"Update application {data['cmdb_name']} environment {env['ENVIRONMENT']} to version {env['NEW_VALUE']}.",
                "justification": "Enhancements, bug fixes, other normal development activity.",
                "implementation_plan": "Deployment will be deployed via ArgoCD.",
                "backout_plan": "We will either use the rollback option in the deployment tool, or the previous version will be redeployed again via the github orchestration process.",
                "test_plan": "Development team will verify application connectivity and operation during deployment/maintenance window.",
                "correlation_display": "GitHub Actions",
                "correlation_id": env['CORRELATION_URL'],
            }
        }

        if not args.simple:
            crtemplate['attributes']['u_devops_endpoint'] = f"/repos/usps/{env['REPO']}/dispatches"
            crtemplate['attributes']['chg_model'] = { "name": "DevOps Simplified" }


        if "default_creator_name" in data.keys():
            requesting_user = crtemplate['attributes']['requested_by'] = data['default_creator_name']
            self.VARMAP['requesting_user'] = requesting_user

        if "default_implementation_plan" in data.keys():
            crtemplate['attributes']['implementation_plan'] = data['default_implementation_plan'].format(**self.VARMAP)

        if "y" in data['pci_in_scope'].lower():
            crtemplate['attributes']['u_functionality_testing'] = data['u_functionality_testing'].format(**self.VARMAP)

        if "y" in data['u_tslc_project'].lower():
            crtemplate['attributes']['u_tslc_project_id'] = { "u_tslc_project_id": data['u_tslc_project_id'] }
            crtemplate['attributes']['u_tslc_activity'] = { "u_tslc_activity": data['u_tslc_activity'] }
            if data['u_tslc_activity'] not in self.tslc_activity_options:
                raise Exception(f"Invalid value for 'u_tslc_activity' in cr_template.yaml. Must be (case sensitive) one of: {self.tslc_activity_options}")

        if "default_short_description" in data.keys():
            crtemplate['attributes']['short_description'] = data['default_short_description'].format(**self.VARMAP)

        if "default_description" in data.keys():
            crtemplate['attributes']['description'] = data['default_description'].format(**self.VARMAP)

        if "default_justification" in data.keys():
            crtemplate['attributes']['justification'] = data['default_justification'].format(**self.VARMAP)

        if "default_backout_plan" in data.keys():
            crtemplate['attributes']['backout_plan'] = data['default_backout_plan'].format(**self.VARMAP)

        if "default_test_plan" in data.keys():
            crtemplate['attributes']['test_plan'] = data['default_test_plan'].format(**self.VARMAP)

        return crtemplate

    def generate_implementation_plan(self):
        data = self.load_user_template()

        u_devops_endpoint = f"/repos/usps/{env['REPO']}/dispatches"

        u_devops_payload = {
            "event_type": "SNOW Deployment Request Callback",
            "client_payload": {
                "pr_number": env['PR_NUMBER'],
                "cr_number": env['CR_NUMBER'],
                "server": env['SNOW_SERVER_ENV'],
                "deployenv": env['ENVIRONMENT'],
                "argocd_server": self.argocd_server,
                "argocd_apps": env['ARGOCD_APPS']
            }
        }

        if args.simple:
            containerized_implementation_plan = {}
        else:
            containerized_implementation_plan = {
                "u_devops_endpoint": u_devops_endpoint,
                "u_devops_payload": json.dumps(u_devops_payload),
            }

        return containerized_implementation_plan

    def cr_template(self, filepath):
        with open(filepath, "w") as f:
            f.write(json.dumps(self.generate_cr_template()))

    def implementation_plan_update(self, filepath):
        with open(filepath, "w") as f:
            f.write(json.dumps(self.generate_implementation_plan()))


class argocd_syncer:
    REQ_VAR = [ "ARGOCD_TOKEN", "ARGOCD_SERVER", "ARGOCD_APPS", "ENVIRONMENT", "REPO" ]
    yaml = ruamel.yaml.YAML()

    def __init__(self):
        for v in self.REQ_VAR:
            if v not in env.keys():
                raise Exception(f"Missing environment variable: {v} must be defined.")
        self.argocd_server = env['ARGOCD_SERVER'].format(env = env['ENVIRONMENT'].lower())
        if "https://" not in self.argocd_server:
            self.argocd_server = "https://" + self.argocd_server
        self.argocd_headers = { "Content-type": "application/json", "Authorization": "Bearer " + env['ARGOCD_TOKEN'] }
        self.load_user_template()
        self.populate_applist()
        return

    def printlist(self, thelist, theheader):
        if len(thelist) == 0:
            return
        print(theheader, file=sys.stderr)
        for item in thelist:
            print(" ", item, file=sys.stderr)
        print("", file=sys.stderr)
        return

    def load_user_template(self):
        dir_path = path.dirname(path.realpath(__file__))
        cr_creation_path = f"{dir_path}{sep}..{sep}..{sep}cr_creation.yml"
        cr_creation_path = path.realpath(cr_creation_path)

        if not path.exists(cr_creation_path):
            raise Exception(f"Missing {cr_creation_path}")

        with open(cr_creation_path) as f:
            data = self.yaml.load(f)

        self.user_template_data = data
        return

    def execute(self):
        self.init_argo_syncs()
        return self.monitor_argo_syncs()

    def populate_applist(self):
        # Determine which applications to sync, either based on git repo or from the user provided list.
        argocd_apps_path = f"{self.argocd_server}/api/v1/applications"
        argocd_applications = requests.get( argocd_apps_path, headers = self.argocd_headers, verify = False )
        print(f"Retrieved app list from {self.argocd_server} with code {argocd_applications.status_code}.", file=sys.stderr)
        if argocd_applications.status_code > 299:
            exit(1)
        argocd_applications = argocd_applications.json()['items']

        if env['ARGOCD_APPS'].strip() == "*":
            self.apps_to_sync = [ app['metadata']['name'] for app in argocd_applications if env['REPO'].lower() in app['spec']['source']['repoURL'].lower() ]
            self.printlist(self.apps_to_sync, f"We will be syncing the following applications on {self.argocd_server}\nDiscovered Application List:")
        else:
            user_provided_applist = [ app.strip().lower() for app in env['ARGOCD_APPS'].split(',') ]
            self.apps_to_sync = [ app['metadata']['name'] for app in argocd_applications if app['metadata']['name'].lower() in user_provided_applist ]
            self.printlist(self.apps_to_sync, f"We will be syncing the following applications on {self.argocd_server}\nVerified User-Provided Application List")

        # Append template list if present and dedup.
        if "default_argocd_applist" in self.user_template_data.keys():
            self.apps_to_sync += default_argocd_applist['default_argocd_applist']
            self.apps_to_sync = list(set(self.apps_to_sync))

        if len(self.apps_to_sync) == 0:
            print("No applications were found to sync.", file=sys.stderr)
            exit(1)
        self.apps_to_sync.sort()

    def init_argo_syncs(self):
        for app_name in self.apps_to_sync:
            sync_endpoint_url = f"{self.argocd_server}/api/v1/applications/{app_name}/sync"
            r = requests.post(sync_endpoint_url, headers = self.argocd_headers, verify = False)
            if r.status_code > 299:
                print(f"Error communicating with the ArgoCD Server: {r.text}", file=sys.stderr)
                exit(1)
            else:
                print(f"Initiated Sync for {app_name}.", file=sys.stderr)
        return

    def monitor_argo_syncs(self):
        argo_api_status_path = "/api/v1/applications/{application_name}"

        Success = True
        while True:
            Degraded = False
            syncs_in_progress = False
            for app_name in self.apps_to_sync:
                print(f"Checking app sync status for {app_name}.", file=sys.stderr)
                app_status_endpoint_url = self.argocd_server + argo_api_status_path.format(application_name = app_name)
                app_status = requests.get(app_status_endpoint_url, headers = self.argocd_headers, verify = False)

                # Check to see if there is a sync in progress.
                if app_status.json()['status']['sync']['status'] == "Progressing":
                    syncs_in_progress = True
                    print(f"Sync in progress for {app_name}. {app_status.json()['status']['sync']['status']}.", file=sys.stderr)
                else:
                    print(f"Sync complete for {app_name}. {app_status.json()['status']['sync']['status']}.", file=sys.stderr)
                    # If the sync is complete, check application status and catch unhealthy applications.
                    if app_status.json()['status']['health']['status'] != "Healthy":
                        Degraded = True
                    print(f"Health status: {app_status.json()['status']['health']['status']}.", file=sys.stderr)

            # Check to see if all applications are synced.
            # If an application is degraded, report an unsuccessful sync.
            if not syncs_in_progress:
                if Degraded:
                    Success = False
                break
            sleep(15)

        # All syncs are done.
        # Dump final status to logs now that syncs are completed.
        for app_name in self.apps_to_sync:
            app_status_endpoint_url = self.argocd_server + argo_api_status_path.format(application_name = app_name)
            app_status = requests.get(app_status_endpoint_url, headers = self.argocd_headers, verify = False)
            if app_status.json()['status']['health']['status'] != "Healthy":
                for resource in app_status.json()['status']['resources']:
                    print(resource['kind'], resource['name'], file=sys.stderr)
                    for i in resource['health'].keys():
                        print(" ", i, ":", resource['health'][i], file=sys.stderr)
            else:
                print(app_name, "is healthy.", file=sys.stderr)

        print("", file=sys.stderr)
        if Success:
            print("Sync Successful.", file=sys.stderr)
            return Success
        else:
            print("Sync Completed with errors.", file=sys.stderr)
            return Success


if __name__ == "__main__":
    dir_path = path.dirname(path.realpath(__file__))

    if args.imageupdate:
        x = deployment_updater()
        x.update_deployments()
        exit()
    elif args.secretname:
        print( re.sub(r'\W+', '', env['ARGOCD_SERVER'].format(env = env['ENVIRONMENT']).replace("https://","").replace("/","")).upper() )
        exit()
    elif args.cr_json:
        x = templater()
        x.cr_template(f"{dir_path}{sep}cr.json")
        exit()
    elif args.cr_update:
        x = templater()
        x.implementation_plan_update(f"{dir_path}{sep}imp.json")
        exit()
    elif args.deploy:
        x = argocd_syncer()
        if x.execute():
            print("Deployment suceeded.")
            exit(0)
        else:
            print("Deployment completed with errors. See Logs.")
            exit(1)
    elif args.merge:
        x = pr_merger()
        exit(x.check_and_merge())
