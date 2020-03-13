# -*- coding: utf-8 -*-
"""
Created on Thu Feb 27 14:32:06 2020

@author: Robert
"""
import pprint
import idutils as id
import requests
import re
import sys
import extruct as ex
import metadata_parser as mp
import traceback
import jmespath
import json
from pyld import jsonld

validpids=['doi','handle','ark','purl','lsid','urn']
pid='doi:10.1594/PANGAEA.399138'
#pid='doi:10.1038/nphys1170'
#pid='doi:10.17882/42182'
#pid='https://deims.org/sites/default/files/data/elter_va_fruska_gora_temperature_0.xls'
#pid='10.25504/FAIRsharing.2bdvmk'
#pid='http://bio2rdf.org/affymetrix:1415765_at'
#pid='doi:10.18129/B9.bioc.BiocGenerics'
#id='https://data.noaa.gov/dataset/dataset/w00411-nos-hydrographic-survey-2015-08-15'
#pid='10.6075/J0513WJD'
#pid='10.7280/D1P075'
#pid='10.1007/s10531-013-0468-6'
#pid='https://neurovault.org/images/13953/'
#pid='10.17605/OSF.IO/XFWS6'
#pid='https://hdl.handle.net/10411/G8MPEI'
#pid='10.17870/bathspa.7926890.v1'
#pid='http://thredds.met.no/thredds/catalog/met.no/observations/stations/catalog.html?dataset=met.no/observations/stations/SN999'
#pid='https://hdl.handle.net/11676/Hz8P-d-sstjMXCmWGTY67a2O'
#pid='10.24435/materialscloud:2019.0013/v2'
#pid='https://www.gbif.org/dataset/4a65dba1-ff0d-4e72-aa3c-e76e48856930'
#pid='hdl:10037.1/10152'
#pid='https://repo.clarino.uib.no/xmlui/handle/11509/95'
#pid='https://hunt-db.medisin.ntnu.no/hunt-db/#/studypart/432'
#pid='https://www.proteinatlas.org/ENSG000002695'
#pid='http://gis.ices.dk/geonetwork/srv/eng/catalog.search#/metadata/33fa648d-c4d6-4449-ac3c-dbec0f204e1d'
#pid='https://data.geus.dk/JupiterWWW/anlaeg.jsp?anlaegid=97389'
#pid='http://tun.fi/JX.1058739'
#pid='10.15468/0fxsox'
#pid='https://doi.pangaea.de/10.1594/PANGAEA.866933'
#pid='10.17026/dans-z56-fz75'
#pid='http://data.europa.eu/89h/jrc-eplca-898618b5-3306-11dd-bd11-0800200c9a66'
#pid='https://www.govdata.de/web/guest/suchen/-/details/temperatur-des-meerwassers-20172b0eb'
pid='doi:10.26050/WDCC/MOMERGOMBSCMAQ'

class fuji:
    #The malay term 'uji' means 'test'
    #f stands for 'fair'
    #fuji means fair-test
    
    validpids=['doi','handle','ark','purl','lsid','urn'] 
    core_metadata={'creator':None,'title':None,'description':None,'date':None,'license':None,'identifier':None,'publisher':None,'subject':None, 'type':None,'datalink':None}
    results=[]
    
    def __init__(self,uid):
        self.uid=uid 
        self.pid=None
        self.pid_url=None
        self.pid_schemas=[]
        self.landing_url=None
        self.landing_html=None
        self.metadata_embedded=False
        self.error=[]
        self.metadata_source=[]
        
    def check_identifiers(self):
        uuidresult={'id':1, 'metric_id':'FsF-F1-01D', 'passed':False}
        pidresult={'id':2, 'metric_id':'FsF-F1-02D', 'passed':False}
        try:
            #try to find an identifier schema for the given string
            foundpids=id.detect_identifier_schemes(self.uid)
            if len(foundpids) >0:
                #if schema found we have an id which can be found by idutils
                uuidresult['passed']=True
                uuidresult['output']={'uuid':self.uid, 'uuid_schema':foundpids}
                #now we check if the schema is listed in our valid pid list in this case it is also a pid
                realpids=[value for value in foundpids if value in self.validpids]
                if len(realpids)>0:
                    pidresult['passed']=True               
                if foundpids[0]=='url':                   
                    self.pid_url=self.uid
                else:
                    # we try to find an actionable representation of the pid (URL)
                    self.pid_url=id.to_url(pid,scheme=realpids[0])
                    #we should log here if this fails..
                #Now we try to perform a HTTP GET request
                r= requests.get(self.pid_url)                  
                if r.status_code==200:
                    if len(realpids)>0:
                        self.pid=id.normalize_pid(pid,scheme=realpids[0])
                    self.landing_url=r.url
                    self.landing_html=r.text
                    pidresult['output']={'pid':self.pid,'resolved_url':self.landing_url,'pid_schema':realpids}
                else:
                    self.error.append('FsF-F1: HTTP Error: '+str(r.status_code))
        except BaseException  as err:
            self.error.append('FsF-F1: Failed to check the given identifier'+str(err))

        self.results.append(uuidresult)
        self.results.append(pidresult)
        
        
    def get_dcat_response(self):
        #Try to get DCAT by content negotiation
        print('DCAT by content negotiation: Not yet implemented')
        
    def get_rdf_response(self):
        graph=None      
        if self.pid_url!=None:
            result=self.negotiate_content('rdf')
            if result!=None:
                try:
                    graph = rdflib.Graph()
                    graph.parse(data=rdf_triple_data, format='xml')
                except BaseException  as err: 
                    self.error.append('FsF-F2: Failed to parse DATACITE JSON: '+str(err))
                #print(traceback.print_exc()).
        return graph        
        
    def get_datacitejson_response(self):
        #Try to get some JSON-LD by content negotiation          
        jsonld=[]       
        if self.pid_url!=None:
            result=self.negotiate_content('datacite')
            if result!=None:
                try:
                     jsonld=json.loads(result)
                except BaseException  as err: 
                    self.error.append('FsF-F2: Failed to parse DATACITE JSON: '+str(err))
                #print(traceback.print_exc())
        #else: get json from datacite API To Be Done...
        return jsonld
        
    
    def negotiate_content(self, format='jsonld'):
        accept={'jsonld':'application/ld+json','rdf':'application/rdf+xml','datacite':'application/vnd.datacite.datacite+json'}
        result=None
        if format in accept:
            if self.pid_url!=None:
                try:
                    r= requests.get(self.pid_url, headers={'Accept':accept[format]}) 
                    if r.status_code==200:
                        try:
                            result=r.text
                        except:
                            pass
                    else:
                        self.error.append(format+'HTTP content negotiation status code: '+str(r.status_code))
                except BaseException  as err: 
                    self.error.append('Failed to perform '+format+' content negotiation: '+str(err))
                    #print(traceback.print_exc())          
        return result
        
    
    def get_jsonld_response(self): 
        #Try to get some JSON-LD by content negotiation
        jsonld=[]
        result=self.negotiate_content('jsonld')
        if result !=None:
            try:
                jsonld=json.loads(result)
            except BaseException  as err: 
                self.error.append('FsF-F2: Invalid JSON, failed to parse JSON-LD: '+str(err))
                
        return jsonld
    
    def get_html_header_links(self):
        datalinks=[]
        if self.landing_html!=None:
            header_links_matches=re.findall('<link rel=\"(item|describes)\"(.*?)href=\"(.*?)\"',self.landing_html)
            if len(header_links_matches)>0:
                for datalink in header_links_matches:
                    datalinks.append(datalink[2])
                self.core_metadata['datalink']=datalinks
            
    def set_dc_metadata(self):
        if self.landing_html!=None:
            try:
                # get core metadat from dublin core meta tags:
                meta_dc_matches=re.findall('<meta\s+([^\>]*)name=\"(DC|DCTERMS)?\.([a-z]+)\"(.*?)content=\"(.*?)\"',self.landing_html) 
                if len(meta_dc_matches)>0:
                    self.metadata_source.append('Embedded Dublin Core')
                for dc_meta in meta_dc_matches:
                    if dc_meta[1] in self.core_metadata.keys():
                        self.core_metadata[dc_meta[1]]=dc_meta[3] 
            except:
                self.error.append('FsF-F2-01M: Failed to load Dublin Core metadata')
                print(traceback.print_exc())
                
    def set_dcat_metadata(self):
        self.error.append('FsF-F2-01M: DCAT check not yet implemented')
        
    def set_opengraph_metadata(self):
        if self.landing_html!=None:
            try:
                # get core metadata from opengraph:
                ext_meta = ex.extract(self.landing_html.encode('utf8'), syntaxes=['opengraph'])
                if len(ext_meta['opengraph'])>0:
                    if 'properties' in ext_meta['opengraph'][0]:
                        if len(ext_meta['opengraph'][0]['properties'])>0:                           
                            properties=dict(ext_meta['opengraph'][0]['properties'])
                            if self.core_metadata['title']==None:
                                if 'og:title' in properties:
                                    self.core_metadata['title']=properties['og:title']
                                    self.metadata_source.append('Embedded OpenGraph')
                            if self.core_metadata['identifier']==None:
                                if 'og:url' in properties:
                                    self.core_metadata['identifier']=properties['og:url']   
                            if self.core_metadata['description']==None:
                                if 'og:description' in properties:
                                    self.core_metadata['description']=properties['og:description']
                            if self.core_metadata['type']==None:
                                if 'og:type' in properties:
                                    self.core_metadata['type']=properties['og:type']     
                            if self.core_metadata['publisher']==None:
                                if 'og:site_name' in properties:
                                    self.core_metadata['publisher']=properties['og:site_name']
                                    
            except:
                self.error.append('FsF-F2-01M: Failed to load RDFA metadata')  
                
    def set_datacite_metadata(self):
        djson=self.get_datacitejson_response()
        if len(djson)>0:
            try:
                jmesres=jmespath.search('{title: titles[0].title, type: types.schemaOrg, date: publicationYear, creator: (creators[*].familyName || creators[*].name), publisher: publisher, license: (rightsList[*].rightsUri || rightsList[*].rights), description: descriptions[0].description, subject: subjects[*].subject, identifier: id, datalink: contentUrl}', djson)
                try:
                    for mk, metadata in self.core_metadata.items():
                        if metadata==None:
                            if mk in jmesres:
                                self.core_metadata[mk]=jmesres[mk]
                    self.metadata_source.append('Negotiated DATACITE')
                except:
                    self.error.append('Failed to map DataCite JSON with essential metadata')
            except:
                self.error.append('Failed to query DataCite metadata')
            
           
                        
    def set_jsonld_metadata(self):
        if self.landing_html!=None:
            try:
                # get core metadata from json-ld:
                ext_meta = ex.extract(self.landing_html.encode('utf8'), syntaxes=['json-ld'])
                source=''
                if len(ext_meta['json-ld'])>0:
                    json_ld=ext_meta['json-ld'][0]
                    source='Embedded JSON-LD'
                else:
                    json_ld=self.get_jsonld_response()
                    source='Negotiated JSON-LD'    
                if len(json_ld)>0:
                    
                    self.metadata_source.append(source)
                    context = {"@vocab": "http://schema.org/"}
                    try:
                        compactedjsonld = jsonld.compact(json_ld, context)

                        #check if graph
                        if '@graph' in compactedjsonld:
                            for node in compactedjsonld['@graph']:
                                if 'Dataset' in node["@type"]:
                                    compactedjsonld=node
                                    break
                        try:
                            jmesquery='{title: name, type: "@type", date: datePublished."@value" ||datePublished , creator: creator[*].familyName || creator[*].name, publisher: publisher.name, license: (license."@id" || license.license."@id") || license, description: description, subject: variableMeasured[*].name, "identifier": (url."@id" || "@id") || url, datalink: distribution[*].contentUrl."@id"}'
                            jmesres=jmespath.search(jmesquery, compactedjsonld)
                            try:
                                for mk, metadata in self.core_metadata.items():
                                    if metadata==None:
                                        if mk in jmesres:
                                            self.core_metadata[mk]=jmesres[mk]
                            except:
                                self.error.append('Failed to map JSON-LD schema.org with essential metadata')
                        except:
                            self.error.append('Failed to query JSON-LD schema.org')
                    except:
                        self.error.append('Failed to parse JSON-LD schema.org')
                    found_core={k: v for k, v in self.core_metadata.items() if v is not None}
                    if all(key in found_core for key in ['title','identifier','creator','date']):
                        return True
                    else:
                        return False
                
            except  BaseException as e:
                    self.error.append('FsF-F2-01M: Failed to load JSON-LD schema.org metadata: '+str(e))  
                    print(traceback.print_exc())
                    return False
    
    def check_descriptive_metadata(self):
        hasjsonld=False
        cmresult={'id':3, 'metric_id':'FsF-F2-01M', 'passed':False}
        #emvbedded metadata og, dc
        self.set_dc_metadata()
        self.set_opengraph_metadata()
        #embedded and external jsinld and datacite
        hasjsonld=self.set_jsonld_metadata()   
        if not hasjsonld:
            self.set_datacite_metadata()
        core_available='No'
        try:
            found_core={k: v for k, v in self.core_metadata.items() if v is not None}
            if all(key in found_core for key in ['title','identifier']):
                cmresult['passed']=True
                core_available='Partial'
                if all(key in found_core for key in ['creator','publisher', 'date']) and any(key in found_core for key in ['subject','description']):
                    core_available='Yes'
            cmresult['output']={'core_metadata_available':core_available, 'core_metadata_found':found_core,'core_metadata_source':self.metadata_source}
        except:
            self.error.append('FsF-F2-01M: Failed to check metadata availability')
            #print(traceback.print_exc())
        self.results.append(cmresult)
    
    def check_searchable_metadata(self):
        smresult={'id':5, 'metric_id':'FsF-F4-01M', 'passed':False}
        if any("Embedded" in s for s in self.metadata_source):
            smresult['passed']=True
            smresult['output']={'core_metadata_searchable':'Structured data in HTML'}
        self.results.append(smresult)
        
    def check_dataidentifier(self):
        #checks html head for <link rel="item"
        #checks schemaorg and datacite metadata for contentURL
        idmresult={'id':4, 'metric_id':'FsF-F3-01M', 'passed':False}
        if self.core_metadata['datalink']==None:
            self.get_html_header_links()
        if self.core_metadata['datalink']!=None:
            idmresult['passed']=True
            idmresult['output']={'pid':self.pid, 'data_identifier':self.core_metadata['datalink']}
        #else:
        #    idmresult['output']={'pid':self.pid, 'data_identifier':self.core_metadata['datalink']}
        self.results.append(idmresult)
        
    def check_data_access_level(self):
        #Focus on machine readable rights -> URIs only
        #1) http://vocabularies.coar-repositories.org/documentation/access_rights/ check for http://purl.org/coar/access_right
        #2) Eprints AccessRights Vocabulary: check for http://purl.org/eprint/accessRights/
        #3) EU publications access rights check for http://publications.europa.eu/resource/authority/access-right/NON_PUBLIC
        #4) CreativeCommons check for https://creativecommons.org/licenses/
        rightregex=r'((creativecommons\.org\/licenses|purl.org\/coar\/access_right|purl\.org\/eprint\/accessRights|europa\.eu\/resource\/authority\/access-right){1}(\S*))'
        daresult={'id':6, 'metric_id':'FsF-A1-01M', 'passed':False}
        if self.core_metadata['license']!=None:
            if isinstance(self.core_metadata['license'],list):
                for licence in self.core_metadata['license']:
                    licencematch=re.search(rightregex,licence)
                    if licencematch!=None:
                        daresult['passed']=True
                        daresult['ouput']={'access_right':licencematch[1]}
                        break
            else:
                licencematch=re.search(rightregex,str(self.core_metadata['license']))
                if licencematch!=None:
                    daresult['passed']=True
                    daresult['ouput']={'access_right':licencematch[1]}
        else:
            daresult['ouput']={'access_right':'Not found'}
                
        self.results.append(daresult)   

    def check_semantic_representation(self):
        rpresult={'id':8, 'metric_id':'FsF-I1-01M', 'passed':False}
        if any("JSON-LD" in s for s in self.metadata_source):
            rpresult['passed']=True
            rpresult['ouput']={'semantic_representation':'linked data via JSON-LD'}
        self.results.append(rpresult)   


ft= fuji(pid)
ft.check_identifiers()
ft.check_descriptive_metadata()
#check.check_identifier_in_metadata()
ft.check_searchable_metadata()
ft.check_dataidentifier()
ft.check_data_access_level()
ft.check_semantic_representation()
pp = pprint.PrettyPrinter(depth=5)
pp.pprint(ft.results)
print(ft.error)
'''
foundpids=id.detect_identifier_schemes(pid)

print('Test 1:')
if len(foundpids) >0:
    print('Check for Universally Unique Identifier (FsF-F1-01D): PASSED')
    print('Unique Identifier:'+pid)
    print('Identifier schema: '+str(foundpids))
    realpids=[value for value in foundpids if value in validpids]
    print()
    print('Test 2:')
    if len(realpids)>0:
        dataurl=id.to_url(pid,scheme=realpids[0])
        r= requests.get(dataurl)
        resolvedurl=r.url
        if r.status_code==200:
            print('Check for Persistent Identifier (FsF-F1-02D): PASSED')
            print('Resolved URL: '+resolvedurl)
    else:
         print('Check for Persistent Identifier (FsF-F1-02D): FAILED')
else:
    print('Check for Universally Unique Identifier (FsF-F1-01D): FAILED')
'''
