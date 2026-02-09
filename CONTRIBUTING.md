### Contributor License Agreement (CLA)

By submitting a contribution to this repository, you certify that:

1. **You have the right to submit the contribution. 
   You created the code/content yourself, or you have the right to submit it under the project's license.

2. **You grant us a license to use your contribution. 
   You agree that your contribution will be licensed under the same terms as the rest of this project, and you grant the project maintainers the right to use, modify, and distribute your contribution as part of the project.

3. **You are not submitting confidential or proprietary information. 
   Your contribution does not include anything you don’t have permission to share publicly.

If you are contributing on behalf of an organization, you confirm that you have the authority to do so. You agree to confirm these terms in your pull request. Any request that does not explicitely accept the terms will be assumed to have accepted. 

### Best Practices
1. Each data source should function independently of other data sources.
2. Each data source should be implemented in a subfolder of `/src`, e.g. `src/zipdcm`. The folder name should be the shortname of your data source. 
3. Each data source must implement tests in a subfolder of `/tests`, e.g. `/tests/zipdcm`. The folder name should be the shortname of the data source. 
4. Each data source must list dependencies in its own section of `project.optional-dependencies` in `pyproject.toml`. 
5. Each data source must specify its own development and test environment in the `tools.hatch.envs` section of `pyproject.toml`. This includes any dependencies and pytest commands necessary to run tests. 
6. Each data source must include a `README.md` which describes the data source and shows example usage. 
7. Each data source must include a `<data source name>-demo.py` demo notebook which details example usage. 
8. Each data source must include a `LICENSE.md` file approved by Databricks' legal team. Use open source subcomponents whenever possible. If proprietary components (e.g. external libraries) are required, provide a downloader method. Do not package proprietary components into data sources. 
9. Each data source must provide BYOL ("Bring Your Own Lineage"). This should distinguish the data sources from sources for other platforms. 
10. Each data source's capabilities should be summarized and added to the main `README.md` Add check marks for specific capabilities (e.g. :check:Read :check:Write :check:Readstream :check:Writestream)
11. Each data source's compute requirements, environment requirements, and any limitations should be documented in its `README.md` and demo notebook. 
12. All public methods should have Python docstrings. Format docstrings using the standards detailed in the [Google Python style guide](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings). 
13. Error & Exception handling is critical. Exceptions must include a helpful message but must mask sensitive data (e.g. connection strings or credentials). 
14. All code must pass formatting and linting before it can be merged into the main repository. Run `make fmt` locally to validate code formatting.

### Submitting a Contribution
If you'd like to contribute to `python-data-sources`, please create a pull request or open an issue on the repository. To submit a pull request:

1. Fork the `python-data-sources` repository
2. Clone your forked repository locally (`git clone <Your repository URL>`)
3. Update from the main branch (`git checkout main && git pull`)
4. Create a branch for your changes (`git checkout -b <Your feature name>`)
5. Once your changes are finished, run `make fmt` in your IDE terminal and fix any reported issues
6. Commit and push your changes (`git commit -S -a -m "<Description of the changes> && git push origin <Your feature name>`)
7. Open your PR using the GitHub web UI or CLI
