import argparse
import json
import io
import os
import logging

from collections import defaultdict

import pycountry
import pycountry_convert
import zstd
import zstandard as zstd
import numpy as np

from geopy.geocoders import Nominatim
from tqdm import tqdm

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-file')
    parser.add_argument('--output-file', default='just_africa_tweets')
    return parser.parse_args()


def process_file(input_file, output_file):
    logger.info('Processed called on {}'.format(input_file))

    num_of_tweets = 0
    num_of_geographic_tweets = 0 
    geolocator = Nominatim()

    africa_data = []
    num_of_africa_tweets = 0
    analysis = defaultdict(lambda: 0)

    with open(input_file, 'rb') as fh:
        dctx = zstd.ZstdDecompressor()
        streamer = dctx.stream_reader(fh)
        wrap = io.TextIOWrapper(io.BufferedReader(streamer), encoding='utf-8')

        with tqdm(total=os.path.getsize(input_file)) as pbar:
            for line in wrap:
                pbar.update(fh.tell() - pbar.n)
                try:
                    tweet = json.loads(line)
                except Exception as e:
                    logger.error('Could not decode line: {}'.format(line))
                    continue

                num_of_tweets += 1

                if 'coordinates' in tweet:
                    num_of_geographic_tweets += 1
                else:
                    continue

                long_lat = str(tweet['coordinates']['coordinates'][1]) + ', ' + str(tweet['coordinates']['coordinates'][0])

                try:
                    location = geolocator.reverse(long_lat, timeout=30)
                except Exception as e:
                    logger.error('Could not parse location for record: {}'.format(line), e)
                    continue

                try:
                    country_code = location.raw['address']['country_code'].upper()
                    country_name = pycountry.countries.get(country_code).name
                    continent = pycountry_convert.country_alpha2_to_continent_code(country_code)
                    analysis[country_name] += 1
                    if continent == 'AF':
                        num_of_africa_tweets += 1
                        africa_data.append(tweet)
                except Exception as e:
                    logger.error('Could not process record: {}'.format(line), e)

    logger.info('There are ' + str(num_of_tweets) + ' tweets in this sample.')
    logger.info('There are ' + str(num_of_geographic_tweets) + ' geo-tagged tweets in this sample.')
    logger.info(dict(analysis))
    logger.info('There are ' + str(num_of_africa_tweets) + ' tweets geo-tagged in Africa in  this sample.')

    data = np.array(africa_data)
    np.savez(output_file, data)


def main():
    args = get_args()
    process_file(args.input_file, args.output_file)


if __name__ == '__main__':
    main()
