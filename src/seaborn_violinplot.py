
import pandas as pd
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
import seaborn as sns
import matplotlib.pyplot as plt


sns.set(style="white",palette="pastel")

import sys

if len(sys.argv) > 1 and sys.argv[1] == "-test":
    import os
    df = pd.read_csv("Datasets/DRUG1N.csv")
    x_field = "BP"
    y_field = "Age"
    hue_field = "Cholesterol"
    output_option = 'output_to_screen'
    output_path = '/tmp/foo.html'
    inner = 'stick'
    sz=5
    title_font_size = 32
    title = "Test Test Test Test Test Test"
    color="green"
else:
    import spss.pyspark.runtime
    ascontext = spss.pyspark.runtime.getContext()
    sc = ascontext.getSparkContext()
    sqlCtx = ascontext.getSparkSQLContext()
    df = ascontext.getSparkInputData().toPandas()
    y_field = '%%y_field%%'
    x_field = '%%x_field%%'
    hue_field = '%%hue_field%%'
    inner = '%%inner%%'
    output_option = '%%output_option%%'
    output_path = '%%output_path%%'
    sz = int('%%output_size%%')
    title_font_size = int('%%title_font_size%%')
    title = '%%title%%'
    color='%%color%%'

if hue_field == '':
    hue_field = None

if x_field == '':
    x_field = None

if y_field == '':
    y_field = None

if inner == 'none':
    inner = None

g = sns.violinplot(x=x_field, y=y_field, hue=hue_field, data=df, split=True,
               inner=inner,size=sz,color=color)
sns.despine(left=True)

if output_option == 'output_to_file':
    if not output_path:
        raise Exception("No output path specified")
else:
    from os import tempnam
    output_path = tempnam()+".svg"

sns.plt.title(title,fontsize=title_font_size)
sns.plt.savefig(output_path)

if output_option == 'output_to_screen':
    import webbrowser
    webbrowser.open(output_path)
    print("Output should open in a browser window")
else:
    print("Output should be saved on the server to path: "+output_path)
