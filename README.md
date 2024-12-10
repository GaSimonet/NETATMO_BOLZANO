<<<<<<< HEAD
# Netatmo Data Collection and Quality Control Pipeline

This repository contains tools for collecting temperature data from Netatmo weather stations and applying quality control to the collected data. Below is a step-by-step guide on how to use this pipeline.

## Prerequisites

- Python 3.x
- Netatmo API account and credentials
- Required Python packages (listed in `requirements.txt`)

## Setup

1. Clone the repository:
```bash
git clone https://gitlab.inf.unibz.it/earth_observation_public/CCT/pnrr-return/netatmo_uhi.git

```

2. Install required packages:

I have myself created a environment for the NetAtmo data:
conda env create -f netatmo_py_env.yaml

Alternatively use mamba or conda for creating and manually installing the packages:
$mamba create -n netatmo_py_env python=3.10
$mamba activate netatmo_py_env
$mamba install xarray pandas numpy cartopy netcdf4 requests urllib3 tqdm pathlib typing plotly dataclasses matplotlib seaborn scipy geopandas shapely 

otherwise :

pip install -r requirements.txt



3. Configure Netatmo API credentials:
   - Create a Netatmo developer account at https://dev.netatmo.com/
   - Create a new application to get your credentials
   - Save your credentials in `tokens.json`:
     ```json
     {
         "client_id": "your_client_id",
         "client_secret": "your_client_secret",
         "refresh_token": "your_refresh_token"
     }
     ```
I will soon add the environment.yaml for easing the process 
## Data Collection Process

### Step 1: Get Station List

<<<<<<< HEAD
First the user has to determine the available station in an area by giving a couple of the two opposite corners cordinates

```bash
python3 station_manager.py
=======
```bash
<<<<<<< HEAD
station_manager.py
>>>>>>> 219ae51 (change a bit README)
=======
python3 station_manager.py
>>>>>>> 2ec6b8a (change README.md)
```
Run station_manager.py with the desired corner coordinates 
The ones suggested in brackets are centered around Bolzano city.

### Step 2: Fetch Temperature Data

Run the data fetching script:
```bash
python3 run_fetch.py
```

The script will:
- Ask you to select a station list file
- Start downloading data for each station
- Handle rate limits automatically by:
  1. Trying to refresh the access token
  2. Waiting for an hour if needed
- Save progress and allow resuming if interrupted




- Create CSV files with temperature data in `temperature_station_data` directory

## Quality Control Pipeline

After collecting the data, run the quality control process:

1.CHech if all the data where successfully downloaded

2. Run the quality control pipeline:
```bash
python3 run_qc.py
```
- First you can choose if you want to filter the full period or only a specific time frame.
The QC pipeline includes several tests:

-Seasonal Threshold Test

Checks temperature against seasonal limits (DJF, MAM, JJA, SON)
Different min/max thresholds for each season


-Buddy Check

Compares station with neighbors within a radius
Considers elevation differences
Flags values that deviate too much from neighbors


-Spatial Consistency Test (SCT)

More sophisticated neighbor comparison
Uses inner and outer radius
Considers both horizontal and vertical distances


-Completeness Check

Filters based on data completeness threshold
Applied to both timesteps and stations

this script also create filter_qc_nc files in qc_output_folder


## Output Files
run create_netcdf.py --> gather all the csv files from temperature_data_stations folder into one .nc file

## Tests 

- A unittest has been setup to test the different function of this package:

API Interaction Testing

Tests authentication flow with Netatmo API
Verifies correct handling of API responses
Ensures rate limiting is working


Data Processing Verification

Validates temperature data fetching
Checks data formatting and storage
Confirms quality control filters work correctly


Error Handling

Tests network failure scenarios
Verifies token refresh mechanism
Ensures proper handling of missing data

## Troubleshooting

Common issues and solutions:

1. Rate Limit Exceeded
   - The script will automatically handle rate limits (500 queries per hour)
   - Let it run and it will resume automatically

2. Missing Data
   - Check station availability in the station list
   - Verify API credentials
   - Ensure internet connectivity

3. QC Pipeline Errors
   - Check input data format
   - Verify all required files are present


## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This package is under the GNU General Public License (GPL), see LICENSE.txt for more information.

## Contact

contact gaspard.simonet@eurac.edu for questions and remarks !
=======
# NETATMO_Bolzano



## Getting started

To make it easy for you to get started with GitLab, here's a list of recommended next steps.

Already a pro? Just edit this README.md and make it your own. Want to make it easy? [Use the template at the bottom](#editing-this-readme)!

## Add your files

- [ ] [Create](https://docs.gitlab.com/ee/user/project/repository/web_editor.html#create-a-file) or [upload](https://docs.gitlab.com/ee/user/project/repository/web_editor.html#upload-a-file) files
- [ ] [Add files using the command line](https://docs.gitlab.com/ee/gitlab-basics/add-file.html#add-a-file-using-the-command-line) or push an existing Git repository with the following command:

```
cd existing_repo
git remote add origin https://gitlab.inf.unibz.it/earth_observation_public/CCT/pnrr-return/ts1/netatmo_bolzano.git
git branch -M main
git push -uf origin main
```

## Integrate with your tools

- [ ] [Set up project integrations](https://gitlab.inf.unibz.it/earth_observation_public/CCT/pnrr-return/ts1/netatmo_bolzano/-/settings/integrations)

## Collaborate with your team

- [ ] [Invite team members and collaborators](https://docs.gitlab.com/ee/user/project/members/)
- [ ] [Create a new merge request](https://docs.gitlab.com/ee/user/project/merge_requests/creating_merge_requests.html)
- [ ] [Automatically close issues from merge requests](https://docs.gitlab.com/ee/user/project/issues/managing_issues.html#closing-issues-automatically)
- [ ] [Enable merge request approvals](https://docs.gitlab.com/ee/user/project/merge_requests/approvals/)
- [ ] [Set auto-merge](https://docs.gitlab.com/ee/user/project/merge_requests/merge_when_pipeline_succeeds.html)

## Test and Deploy

Use the built-in continuous integration in GitLab.

- [ ] [Get started with GitLab CI/CD](https://docs.gitlab.com/ee/ci/quick_start/index.html)
- [ ] [Analyze your code for known vulnerabilities with Static Application Security Testing (SAST)](https://docs.gitlab.com/ee/user/application_security/sast/)
- [ ] [Deploy to Kubernetes, Amazon EC2, or Amazon ECS using Auto Deploy](https://docs.gitlab.com/ee/topics/autodevops/requirements.html)
- [ ] [Use pull-based deployments for improved Kubernetes management](https://docs.gitlab.com/ee/user/clusters/agent/)
- [ ] [Set up protected environments](https://docs.gitlab.com/ee/ci/environments/protected_environments.html)

***

# Editing this README

When you're ready to make this README your own, just edit this file and use the handy template below (or feel free to structure it however you want - this is just a starting point!). Thanks to [makeareadme.com](https://www.makeareadme.com/) for this template.

## Suggestions for a good README

Every project is different, so consider which of these sections apply to yours. The sections used in the template are suggestions for most open source projects. Also keep in mind that while a README can be too long and detailed, too long is better than too short. If you think your README is too long, consider utilizing another form of documentation rather than cutting out information.

## Name
Choose a self-explaining name for your project.

## Description
Let people know what your project can do specifically. Provide context and add a link to any reference visitors might be unfamiliar with. A list of Features or a Background subsection can also be added here. If there are alternatives to your project, this is a good place to list differentiating factors.

## Badges
On some READMEs, you may see small images that convey metadata, such as whether or not all the tests are passing for the project. You can use Shields to add some to your README. Many services also have instructions for adding a badge.

## Visuals
Depending on what you are making, it can be a good idea to include screenshots or even a video (you'll frequently see GIFs rather than actual videos). Tools like ttygif can help, but check out Asciinema for a more sophisticated method.

## Installation
Within a particular ecosystem, there may be a common way of installing things, such as using Yarn, NuGet, or Homebrew. However, consider the possibility that whoever is reading your README is a novice and would like more guidance. Listing specific steps helps remove ambiguity and gets people to using your project as quickly as possible. If it only runs in a specific context like a particular programming language version or operating system or has dependencies that have to be installed manually, also add a Requirements subsection.

## Usage
Use examples liberally, and show the expected output if you can. It's helpful to have inline the smallest example of usage that you can demonstrate, while providing links to more sophisticated examples if they are too long to reasonably include in the README.

## Support
Tell people where they can go to for help. It can be any combination of an issue tracker, a chat room, an email address, etc.

## Roadmap
If you have ideas for releases in the future, it is a good idea to list them in the README.

## Contributing
State if you are open to contributions and what your requirements are for accepting them.

For people who want to make changes to your project, it's helpful to have some documentation on how to get started. Perhaps there is a script that they should run or some environment variables that they need to set. Make these steps explicit. These instructions could also be useful to your future self.

You can also document commands to lint the code or run tests. These steps help to ensure high code quality and reduce the likelihood that the changes inadvertently break something. Having instructions for running tests is especially helpful if it requires external setup, such as starting a Selenium server for testing in a browser.

## Authors and acknowledgment
Show your appreciation to those who have contributed to the project.

## License
For open source projects, say how it is licensed.

## Project status
If you have run out of energy or time for your project, put a note at the top of the README saying that development has slowed down or stopped completely. Someone may choose to fork your project or volunteer to step in as a maintainer or owner, allowing your project to keep going. You can also make an explicit request for maintainers.
>>>>>>> 3402472f61c319a5c08cf83cef51aacccce90e3a
