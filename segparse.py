#!/usr/bin/env python

def getTotalLength(fname):
    import wave, contextlib
    with contextlib.closing(wave.open(fname,'r')) as f: return f.getnframes() / float(f.getframerate())

def getValFromSeg(fname):
    fhandle  = open(fname, "r")
    fdata    = fhandle.read().split("\n")[:-1]
    speakers = {}
    i, size  = 0, len(fdata)
    while  i < size:
        if fdata[i][:2] == ";;":
            j = i + 1
            while j < size and fdata[j][:2] != ";;":
                items = fdata[j].split(" ")
                time  = (float(items[2])/100, (float(items[2])+float(items[3]))/100)
                if items[7] not in speakers.keys(): speakers[items[7]] = {
                    'gender': items[4], 'length':float(items[3])/100, 'times': [time]
                }
                else: 
                    speakers[items[7]]['length'] += float(items[3])/100
                    speakers[items[7]]['times'].append(time)
                j += 1
            i = j
        else: i += 1
    fhandle.close()
    return speakers

def exportLabels(speakers, fname):
    
    fhandle  = open(fname, "w")
    for speaker in speakers.items():
        for time in speaker[1]['times']:
            toWrite = str(time[0]) + "\t" + str(time[1]) + "\t" + speaker[0]+'-'+speaker[1]['gender']
            fhandle.write(toWrite+'\n')
    fhandle.close()

def removeOldFiles(files):
    for f in files: os.system("rm "+f)

def convertSegToCtl(seg_file, ctl_file):
    cmd = "java -cp LIUM_SpkDiarization.jar fr.lium.spkDiarization.tools.SPrintSeg --help --sInputMask="+seg_file+" --sOutputMask="+ctl_file+" --sOutputFormat=ctl spkr0"
    os.system(cmd)

def exportResult(res_file, clen, tlen, male, female, score):
    print "#######  Result: "+res_file+" #######"
    
    fhandle  = open(res_file, "w")
    
    toWrite = "Total Length:"+str(tlen)
    print toWrite
    fhandle.write(toWrite+"\n")
    
    toWrite =  "Length of Spoken Part:"+str(clen)
    print toWrite
    fhandle.write(toWrite+"\n")
    
    toWrite =  "Total People:"+str(male+female)
    print toWrite
    fhandle.write(toWrite+"\n")
    
    toWrite =  "Males:"+str(male)
    print toWrite
    fhandle.write(toWrite+"\n")
    
    toWrite =  "Females:"+str(female)
    print toWrite
    fhandle.write(toWrite+"\n")

    toWrite =  "Score:"+str(score)
    print toWrite
    fhandle.write(toWrite+"\n")
    
    fhandle.close()

    import re
    print "#################"+re.sub('[\w\-\.]', '#', res_file)+"########"

def runLIUM(wav_file, seg_file):
    cmd = "/usr/bin/java -Xmx2024m -jar ./LIUM_SpkDiarization.jar --fInputMask=./"+wav_file+" --sOutputMask=./"+seg_file+" --doCEClustering  spkr0"
    # cmd = "/usr/bin/java -Xmx2024m -jar ./LIUM_SpkDiarization.jar --thresholds=1.5:2.5,2.5:3.5,250.0:300,0:3.0 --fInputMask=./"+wav_file+" --sOutputMask=./"+seg_file+" --doTuning=2 --doCEClustering  spkr0"
    # cmd = "/usr/bin/java -Xmx2024m -jar ./LIUM_SpkDiarization.jar --thresholds=1.5:2.5,2.5:3.5,250.0:300,0:3.0 --fInputMask=./"+wav_file+" --sOutputMask=./"+seg_file+" --doCEClustering  spkr0"
    # cmd = "/usr/bin/java -cp LIUM_SpkDiarization.jar fr.lium.spkDiarization.system.Diarization --fInputMask=./"+wav_file+" --sOutputMask=./"+seg_file+" --doCEClustering  spkr0"
    os.system(cmd)

import MySQLdb
def DBConn():
    db = MySQLdb.connect(host="127.0.0.1", user="root", passwd="", db="spkdrz")
    return db

def exportResultToDB(db, req_id, result):
    sql = "INSERT INTO result_table (req_id, total_length, spoken_length, total_speakers, males, females, score) VALUES (%d,%f,%f,%d,%d,%d,%f)"%(int(req_id), float(result[1]), float(result[0]), int(result[2])+int(result[3]), int(result[2]), int(result[3]),float(result[4]))
    c = db.cursor()
    try:
        c.execute(sql)
        db.commit()
        return True
    except MySQLdb.IntegrityError:
        return False

def changeStatusToSuccess(db, req_id):
    sql = "UPDATE surveys SET req_status = 'Completed' WHERE req_id = %d"%req_id
    c = db.cursor()
    try:
        c.execute(sql)
        db.commit()
        return True
    except MySQLdb.IntegrityError:
        return False
    
def updateLastAttemptTime(db, req_id):
    sql = "UPDATE surveys SET time_of_last_attempt = NOW() WHERE req_id = %d"%req_id
    c = db.cursor()
    try:
        c.execute(sql)
        db.commit()
        return True
    except MySQLdb.IntegrityError:
        return False

def updateAttemptCount(db, req_id):
    sql = "UPDATE surveys SET number_of_attempts = number_of_attempts+1 WHERE req_id = %d"%req_id
    c = db.cursor()
    try:
        c.execute(sql)
        db.commit()
        return True
    except MySQLdb.IntegrityError:
        return False

def updateFailCount(db, req_id):
    sql = "UPDATE surveys SET fail_count = fail_count+1 WHERE req_id = %d"%req_id
    c = db.cursor()
    try:
        c.execute(sql)
        db.commit()
        return True
    except MySQLdb.IntegrityError:
        return False

def getRequestsFromDB(db):
    requests = []
    sql = "SELECT req_id, file_name, file_path, job_id, survey_id FROM surveys WHERE req_status = 'Pending' ORDER BY number_of_attempts ASC LIMIT 5"
    c = db.cursor()
    c.execute(sql)
    for row in c.fetchall():
        requests.append({'req_id': int(row[0]), 'fname': row[1], 'fpath': row[2], 'job_id': row[3], 'survey_id': row[4 ] })
    return requests

# def getRequestsFromDB(db):
#     requests = []
#     sql = "SELECT req_id, file_name, file_path, file_id, form_id FROM surveys WHERE req_status = 'Pending' ORDER BY number_of_attempts ASC LIMIT 5"
#     c = db.cursor()
#     c.execute(sql)
#     for row in c.fetchall():
#         requests.append({'req_id': int(row[0]), 'fname': row[1], 'fpath': row[2], 'fid': row[3], 'form_id': row[4] })
#     return requests

def getLiterals():
    import getopt, sys
    if len(sys.argv) < 2:
        print 'test.py -m <mode> -f <filename>'
        exit(0)
    sargs = {}
    try:
        opts, args = getopt.getopt(sys.argv[1:],"m:f:")
    except getopt.GetoptError:
        print 'test.py -m <mode> -f <filename>'
        exit(0)
    for opt, arg in opts:
        sargs[opt.replace("-","")] = arg
    return sargs

def downloadWav(fpath, fname):
    import urllib
    try:
        print "Downloading '"+fname+".wav' from: "+ fpath
        wav = urllib.URLopener()
        wav.retrieve(fpath, fname+".wav")
        return True
    except Exception,e:
        print "Error:", str(e)
        return False

def SFTPTransfer():
    import paramiko

    ssh = paramiko.SSHClient() 
    ssh.load_host_keys(os.path.expanduser(os.path.join("~", ".ssh", "known_hosts")))
    ssh.connect(server, username=r'', password=r'')
    sftp = ssh.open_sftp()
    sftp.put(localpath, remotepath)
    sftp.close()
    ssh.close()
    return False

def post2(job_id, survey_id, result):
    import requests
    from urllib import urlencode
    from requests_toolbelt.utils import dump
    
    params, header = {
        "job_id": job_id,
        "survey_id": survey_id,
        "data": result
    }, {
        "X-API-KEY":"",
        "U-ACCESS-TOKEN":""
    }
    
    url = "http://dev.bapps.pitb.gov.pk/rct_marketplace/api/middleware_job/update_audio_quality"
    print "Posting the data to:", url
    print "data:", params
    
    r = requests.post(url, data=params, headers=header)
    # r = requests.get(url, params=urlencode(params), headers={'Authorization': 'access_token g4oow4wkgk4oc40ssc80ckwg0cg4cwcowoc8w0cg'})
    
    # data = dump.dump_all(r)
    # print(data.decode('utf-8'))
    
    print "Response:", r.text
    return r.text

def post(fid, form_id, result):
    import requests
    
    params, header = {
        "id": fid, 
        "form_id": form_id,
        "total_length":result[1], 
        "spoken_length":result[0], 
        "total_speakers":int(result[2])+int(result[3]), 
        "males":result[2], 
        "females":result[3]
    }, {
        "X-API-KEY":""
    }
    
    url = "http://rct-marketplace.itu.edu.pk/api/middleware_job/audio_stat"
    print "Posting the data to:", url
    print "data:", params
    
    r = requests.post(url, data=params, headers=header)
    print "Response:", r.text

def getPreviousLengthRatios(job_id, survey_id):
    lr = []
    sql  = "SELECT spoken_length / total_length FROM result_table WHERE req_id IN (SELECT req_id FROM surveys WHERE job_id = %s AND survey_id < %s)" % (job_id, survey_id)
    c    = db.cursor()
    c.execute(sql)
    for row in c.fetchall():
        lr.append(float(row[0]))
    return lr

def getScore(clen, tlen, job_id, survey_id):
    
    from scipy import stats

    lr  = getPreviousLengthRatios(job_id, survey_id)
    _lr = clen/float(tlen)

    if len(lr) < 10: return _lr * 0.85

    lr.append(_lr)

    zscore = stats.zscore(lr)

    prob   = stats.norm.cdf(zscore[-1])

    return prob

import os
def process(fname, job_id = False, survey_id = False):
    
    wav_file = fname+".wav"
    seg_file = fname+".seg"
    lbl_file = fname+".txt"
    res_file = fname+".res"
    
    removeOldFiles([seg_file, lbl_file, res_file])
    runLIUM(wav_file, seg_file)

    male, female, clen, tlen, speakers = 0,0,0, getTotalLength(wav_file), getValFromSeg(seg_file)
    for speaker in speakers.values():
        clen += speaker["length"]
        if   speaker["gender"].lower() == "m": male += 1
        elif speaker["gender"].lower() == "f": female += 1
    

    exportLabels(speakers, lbl_file)
    
    score = getScore(clen, tlen, job_id, survey_id)

    exportResult(res_file, clen, tlen, male, female, score)
    
    return [clen, tlen, male, female, score]

if __name__ == "__main__":
    
    post2(2, 43, 0.9)
    exit(0)
    
    args = getLiterals()
    if "f" in args.keys() :
        process(args["f"])        
    if "m" in args.keys() and args["m"] == "db":
        db = DBConn()
        requests = getRequestsFromDB(db)
        for request in requests:
            if "fname" in request.keys() and "req_id" in request.keys() and "fpath" in request.keys():
                updateLastAttemptTime(db, request['req_id'])
                updateAttemptCount(db, request['req_id'])
                if downloadWav(request['fpath'], request['fname']):
                    result = process(request["fname"], request["job_id"], request["survey_id"] )
                    if exportResultToDB(db, request['req_id'], result):
                      changeStatusToSuccess(db, request['req_id'])  
                      # post(request["fid"], request["form_id"], result)
                      post2(request["job_id"], request["survey_id"], result[4])
                    else:
                         updateFailCount(db, request['req_id'])
                    # db.commit()
                else:
                    print "Failed to download file '"+request["fname"]+".wav' from: "+ request["fpath"]
                    updateFailCount(db, request['req_id'])
        db.close()


        