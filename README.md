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

1. To run tmVar3 on a list of PMIDs, run

```bash
python run-ner-v0.1.py --tool tmvar3 -i csv-Bacteroid_BetaSearch_20250821.csv -o <full path of the output directory. eg:out_tmVar3> --ignore-errors 
```

To run BioNExt on a list of PMIDs, run

```bash
python run-ner-v0.1.py --tool bionext -i csv-Bacteroid_BetaSearch_20250821.csv -o <full path of the output directory. eg:out_bionext> --ignore-errors --pipenv-dir <full path of the pipenv dir> --bionext-path <path to bionext main.py>
```

2. To extract mutations from the tmVar3/BioNExt generated BioC files, run

```bash
python extract-mutations.py --file < path to the xml/json file (tmVar3/BioNExt respectively)> --format < one of tmVar3 or bionext> --out <output file name>
```

e.g.:
```bash
python extract-mutations.py --file out_bionext/tagger/pubmed_10960088.json --format bionext --out out_bionext/tagger/pubmed_10960088.json.txt
```

