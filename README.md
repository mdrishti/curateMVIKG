# curateMVIKG
Initial steps to curate data from PMC open access.

## Installation



To install dependencies, run

```bash
pipenv install Pipfile
```


In case of problems with running `paddleocr`, install paddleocr dependency separately by running

```bash
pip install paddlepaddle==3.1.1 -i https://www.paddlepaddle.org.cn/packages/stable/cpu/
```

See also [paddleocr github](https://github.com/PaddlePaddle/PaddleOCR?tab=readme-ov-file#2-installation) and [corresponding installation guide](https://www.paddlepaddle.org.cn/en/install/quick?docurl=undefined)



## Running parts of the codes

To run tmVar3 on a list of PMIDs, run

```bash
python run-tmVar3-v0.1.py --tool tmvar3 -i csv-Bacteroid_BetaSearch_20250821.csv -o out_tmVar3 --ignore-errors > logPMC_tmVar3
```

To run BioNExt on a list of PMIDs, run

```bash
python run-tmVar3-v0.1.py --tool bionext -i csv-Bacteroid_BetaSearch_20250821.csv -o out_bionext --ignore-errors --pipenv-dir <path to pipenv dir> --bionext-path <path to bionext main.py> logPMC_BioNExt
```

