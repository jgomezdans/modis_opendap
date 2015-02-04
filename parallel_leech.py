#!/usr/bin/env python

from multiprocessing.dummy import Pool
import datetime
import sys
import numpy as np
from pydap.client import open_url


def lonlat ( lon, lat, scale_factor=2. ):
    """A simple function to calculate the MODIS tile, as well as
    pixel location for a particular longitude/latitude pair. The
    scale factor relates to the actual MODIS spatial resolution, 
    and its possible values are 1 (=1km data), 2 (=0.5km data) and
    4 (0.25km data)"""
    scale_factor = scale_factor*1.
    sph = 6371007.181
    ulx = -20015109.354
    uly = 10007554.677
    cell_size = 926.62543305
    tile_size = 1200*cell_size
    x = np.deg2rad ( lon )*sph*np.cos(np.deg2rad(lat))
    y = np.deg2rad ( lat)*sph
    v_tile = int ( -( y - uly)/tile_size )
    h_tile = int ( (x - ulx)/tile_size )
    line = (uly-y-v_tile*tile_size)/(cell_size/scale_factor)
    sample = ( x - ulx - h_tile*tile_size)/(cell_size/scale_factor )
    return h_tile, v_tile, int(line), int(sample)

def grab_slave ( input, leech_query=50):
    location, band, i0,time, sample, line = input
    ds = open_url ( location )
    nsteps = len ( time )
    tbins = nsteps/leech_query + 1 # We get 100 bins at a go
    x = np.zeros ( nsteps )
    for tstep in xrange(tbins):
        the_end = min(leech_query,len(time) - (leech_query*(tstep)))
        x[tstep*leech_query:(tstep*leech_query + the_end)] = \
            ds[band][(i0+tstep*leech_query):(i0+tstep*leech_query+the_end), line, sample].squeeze()
    return ( band, x )

def grab_refl_data_parallel ( lon, lat, year ):
    htile, vtile, line, sample = lonlat ( lon, lat, scale_factor=2. )
    # Next line is needed for 1km data, such as angles, QA...
    htile, vtile, line1k, sample1k = lonlat ( lon, lat, scale_factor=1. )
    bands_hk = [ "sur_refl_b%02d_1" % i for i in xrange(1,8) ]
    bands_hk += [ "obscov_500m_1", "QC_500m_1" ]
    bands_1k = [ "SolarZenith_1", "SolarAzimuth_1", \
        "SensorZenith_1", "SensorAzimuth_1" ]
    bands_1k += [ "state_1km_1" ]

    map_struct = []

    for isens, prod in enumerate( [ "MOD09GA.005", "MYD09GA.005"] ):
        location = "http://opendap.cr.usgs.gov/opendap/" + \
                  "hyrax/%s/h%02dv%02d.ncml" % ( prod, htile, vtile ) 
        ds = open_url( location )
        time = ds['time'][:]
        xs = (datetime.date ( year, 1, 1) - datetime.date ( 2000, 1, 1 )).days
        xt = (datetime.date ( year, 12, 31) - datetime.date ( 2000, 1, 1 )).days
        i0 = np.nonzero( time == xs )[0]
        it = np.nonzero( time == xt )[0]
        if len(i0) == 0 or len(it) == 0:
            continue
        time = time[i0:(it+1)]

        for band in bands_hk:
            map_struct.append ( [ location, band, i0,time, sample, line] )
        for band in bands_1k:
            map_struct.append ( [ location, band, i0,time, sample1k, line1k] )
    return map_struct

def grab_refl_data ( lon, lat ):
    htile, vtile, line, sample = lonlat ( lon, lat, scale_factor=2. )
    # Next line is needed for 1km data, such as angles, QA...
    htile, vtile, line1k, sample1k = lonlat ( lon, lat, scale_factor=1. )
    print "Getting tile h%02dv%02d..." % (htile, vtile)
    bands_hk = [ "sur_refl_b%02d_1" % i for i in xrange(1,8) ]
    bands_hk += [ "obscov_500m_1", "QC_500m_1" ]
    bands_1k = [ "SolarZenith_1", "SolarAzimuth_1", \
        "SensorZenith_1", "SensorAzimuth_1" ]
    bands_1k += [ "state_1km_1" ]

    retrieved_data = [{}, {}]
    for isens, prod in enumerate( [ "MOD09GA.005", "MYD09GA.005"] ):
        print "Doing product %s" % prod
        ds = open_url("http://opendap.cr.usgs.gov/opendap/" + \
                "hyrax/%s/h%02dv%02d.ncml" % ( prod, htile, vtile ) )
        print "\tGetting time..."
        sys.stdout.flush()
        time = ds['time'][:]
        retrieved_data[isens]['time'] = time
        n_tbins = len(time)/100 + 1 # We get 100 bins at a go
        
        for band in bands_hk:
            print "\tDoing %s "%band, 
            sys.stdout.flush()
            retrieved_data[isens][band] = np.zeros_like(time)
            for tstep in xrange(n_tbins):
                print "*",
                sys.stdout.flush()
                retrieved_data[isens][band][tstep*100:(tstep+1)*100] = \
                    ds[band][tstep*100:(tstep+1)*100, sample, line].squeeze()
    
        for band in bands_1k:
            print "\tDoing %s "%band, 
            sys.stdout.flush()
            retrieved_data[isens][band] = np.zeros_like(time)
            for tstep in xrange(n_tbins):
                print "*",
                sys.stdout.flush()
                retrieved_data[isens][band][tstep*100:(tstep+1)*100] = \
                    ds[band][tstep*100:(tstep+1)*100, sample1k, line1k].squeeze()
    return retrieved_data

if __name__ == "__main__":
    
    for year in xrange ( 2007, 2015):
        print "Downloading year %d..." % year
        the_data = grab_refl_data_parallel ( -88.29186667, 40.0061, year )
        print "\tStarting slave pool..."
        pool = Pool(5)
        results = pool.map( grab_slave, the_data)
        pool.close()
        pool.join()
        # Now add the DoY to each dataset...
        doys = []
        for dataset in the_data:
            doys.append ( np.array ( [int((datetime.date(2000,1,1)+datetime.timedelta(days=x)).strftime("%j")) for x in dataset[3]] ) )
        Ntime_slots = len( doys[0] ) + len ( doys[15] ) # TERRA & AQUA
    #    out = np.zeros(    Ntime_slots, 7+4+1+1+1+1 ) # 7 bands, 4 angles,1 QA@1K, QA@HK, ObsCov, DoY
        QA_OK=np.array([8,72,136,200,1032,1288,2056,2120,2184,2248])
        qa_mod09 = np.logical_or.reduce([results[13][1]==x for x in QA_OK])
        qa_myd09 = np.logical_or.reduce([results[27][1]==x for x in QA_OK])
        
        print "\tSaving file..."
        fp = open ("Bondville_%04d.txt" % year, 'w' )
        fp.write ("# DoY,Platform,SZA,SAA,VZA,VAA,B01,B02,B03,B04,B05,B06,B07,QA1K,QAHK,ObsCov\n" )
        for doy in doys[1]:
            s = None
            passer = doys[1] == doy
            if passer.sum() == 1 and qa_mod09[passer]:
                s = "%d, %d, %10.4G, %10.4G, %10.4G, %10.4G, %10.4G, %10.4G, %10.4G, %10.4G, %10.4G, %10.4G, %10.4G, %10d, %10d, %10.4G" % \
                    ( doy, 1, results[9][1][passer], results[10][1][passer], results[11][1][passer], results[12][1][passer],
                    results[0][1][passer],results[1][1][passer],results[2][1][passer],results[3][1][passer],\
                    results[4][1][passer],results[5][1][passer],results[6][1][passer], results[13][1][passer],
                    results[8][1][passer],results[7][1][passer] )

                fp.write ( "%s\n" % s )

            passer = doys[14] == doy
            if passer.sum() == 1 and qa_myd09[passer]:
                s = "%d, %d, %10.4G, %10.4G, %10.4G, %10.4G, %10.4G, %10.4G, %10.4G, %10.4G, %10.4G, %10.4G, %10.4G, %10d, %10d, %10.4G" % \
                    ( doy, 2,results[23][1][passer], results[24][1][passer], results[25][1][passer], results[26][1][passer],
                    results[14][1][passer],results[15][1][passer],results[16][1][passer],results[17][1][passer],\
                    results[18][1][passer],results[19][1][passer],results[20][1][passer], results[27][1][passer],
                    results[22][1][passer],results[21][1][passer] )
            
                fp.write ( "%s\n" % s )
        fp.close()
