# Summary

The code in this repository helps identify loan contracts in SEC EDGAR filings among those that are already locally stored on your device.
The code was developed for the below project, which we ask you to cite in case you use our code in your research:

Clatworthy, M., Farkas, M., Lui., R. and M. Scapin "Revisiting Fair Value Accounting and Debt Contracting". Unpublished manuscript.

The code builds on the work of [Li, Neamtiu and Tu (2024): "Do Firms Withhold Loan Covenant Details?" The Accounting Review](https://doi.org/10.2308/TAR-2020-0445). In particular, it implements what they refer to as "first step" and "second step" methods in identifying loan contracts.

# Usage

Customizing this repository would allow the user to identify loan contracts to a desired degree of confidence and then search for phrases and dates within them (or to more generally process their contents).

The [test_filings](https://github.com/mikifarkas/sec-edgar-loan-contracts/tree/master/test_filings) folder contains 200 8-K filings for testing purposes.

[screening_of_filings_for_phrases.py](https://github.com/mikifarkas/sec-edgar-loan-contracts/blob/master/screening_of_filings_for_phrases.py): identifies exhibits and searches for terms indicative of loan contracts. Builds a .CSV file with the results.

[first_step_contracts.py](https://github.com/mikifarkas/sec-edgar-loan-contracts/blob/master/first_step_contracts.py): using the above .CSV file, parses the exhibits with loan terms in more detail and records variables that help determine whether the exhibit is a loan contract. Saves results in a .CSV.

[second_step_contracts.py](https://github.com/mikifarkas/sec-edgar-loan-contracts/blob/master/second_step_contracts.py): using the above .CSV file, parses the exhibits with loan terms in more detail and records variables that help determine whether the exhibit is a loan contract. Saves results in a .CSV.

## Notes

We have used Python 3.13. With Python 3.9 we have encoutered errors when writing results to to csvs. We have tested parsing with [lxml](https://lxml.de/) and [beautifulsoup4](https://pypi.org/project/beautifulsoup4/) found that the former was more efficient in this context.
