{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "6956a262-638c-4326-8ec0-86c2afcba17b",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "(206529, 9)\n",
      "(224489, 9)\n",
      "(2372474, 16)\n"
     ]
    }
   ],
   "source": [
    "import pandas as pd\n",
    "import os \n",
    "from sqlalchemy import create_engine\n",
    "import logging\n",
    "import time\n",
    "\n",
    "logging.basicConfig(\n",
    "    filename=\"logs/ingestion.log\",\n",
    "    level=logging.DEBUG,  \n",
    "    format=\"%(asctime)s - %(levelname)s - %(message)s\",\n",
    "    filemode=\"a\"\n",
    ")\n",
    "\n",
    "engine = create_engine('sqlite:///inventory.db')\n",
    "\n",
    "def ingest_db(df, table_name, engine):\n",
    "    '''This function will ingest the dataframe into database table'''\n",
    "    df.to_sql(\n",
    "        table_name,\n",
    "        con=engine,\n",
    "        if_exists='replace',\n",
    "        index=False,\n",
    "        chunksize=5000,\n",
    "        method=\"multi\"\n",
    "    )\n",
    "\n",
    "\n",
    "def load_raw_data():\n",
    "    '''This function will load the CSVs as dataframe and ingest into db'''\n",
    "    start=time.time()\n",
    "    for file in os.listdir('C:/Users/DELL/Desktop/Vendor/Data/'):\n",
    "        if '.csv' in file:\n",
    "            df=pd.read_csv('C:/Users/DELL/Desktop/Vendor/Data/' + file)\n",
    "            print(df.shape)\n",
    "            ingest_db(df, file[:-4] ,engine)\n",
    "    end=time.time()\n",
    "    total_time = (end-start)/60\n",
    "    logging.info('--------------------Ingestion Complete-----------------------')\n",
    "\n",
    "    logging.info(f'\\nTotal Time Taken: {total_time} minutes')\n",
    "\n",
    "if __name__ == '__main__':\n",
    "    load_raw_data()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "0b0ab4d5-0e61-4b9d-afb0-a1fb653755b8",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python [conda env:base] *",
   "language": "python",
   "name": "conda-base-py"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.12.7"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
