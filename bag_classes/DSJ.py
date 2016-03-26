# DSJ bag class

import uuid, json, os
import bagit
from lxml import etree


# define required `BagClass` class
class BagClass(object):
	
		
	# class is expecting a healthy amount of input from `ingestWorkspace` script, and object row
	def __init__(self, object_row, ObjMeta, bag_root_dir, files_location, MODS, MODS_handle, struct_map, object_title, DMDID, collection_identifier, purge_bags):

		# hardcoded
		self.name = 'DSJ' # human readable name, ideally matching filename, for this bag creating class 
		self.content_type = 'WSUDOR_WSUebook' # not required, but easy place to set the WSUDOR_ContentType 

		# passed
		self.object_row = object_row # handle for object mysql row in 'ingest_workspace_object' 
		self.ObjMeta = ObjMeta # ObjMeta class from ouroboros.models
		self.bag_root_dir = bag_root_dir # path for depositing formed bags
		self.files_location = files_location # location of files: they might be flat, nested, grouped, etc.
		self.MODS = MODS # MODS as XML string
		self.MODS_handle = MODS_handle
		self.struct_map = struct_map # JSON representation of structMap section from METS file for this object
		self.object_title = object_title
		self.DMDID = DMDID # object DMDID from METS, probabl identifier for file (but not required, might be in MODS)
		self.collection_identifier = collection_identifier # collection signifier, likely suffix to 'wayne:collection[THIS]'
		self.purge_bags = purge_bags

		# derived
		# MODS_handle (parsed with etree)
		try:
			MODS_tree = etree.fromtring(self.MODS)
			MODS_root = self.MODS_handle.getroot()
			ns = MODS_root.nsmap
			self.MODS_handle = MODS_root.xpath('//mods:mods', namespaces=ns)[0]
		except:
			print "could not parse MODS from DB string"			

		# future
		self.objMeta_handle = None

		# generate obj_dir
		self.obj_dir = "/".join( [bag_root_dir, str(uuid.uuid4())] ) # UUID based hash directory for bag
		if not os.path.exists(self.obj_dir):
			# make root dir
			os.mkdir(self.obj_dir)
			# make data dir
			os.mkdir("/".join([self.obj_dir,"datastreams"]))		


	def createBag(self):

		'''
		Function to create bag given inputs.  Most extensive and complex part of this class.
		'''

		# set identifier
		full_identifier = self.DMDID
		print full_identifier

		# generate PID
		PID = "wayne:%s" % (full_identifier)

		# write MODS
		with open("%s/MODS.xml" % (self.obj_dir), "w") as fhand:
			fhand.write(self.MODS)


		# construct
		################################################################
		
		# get identifier
		identifier = self.MODS_handle['MODS_element'].xpath('//mods:identifier[@type="local"]', namespaces=self.MODS_handle['MODS_ns'])[0].text
		print "identifier: %s" % identifier

		# get volume / issue
		volume = self.MODS_handle['MODS_element'].xpath('//mods:detail[@type="volume"]/mods:number', namespaces=self.MODS_handle['MODS_ns'])[0].text
		issue = self.MODS_handle['MODS_element'].xpath('//mods:detail[@type="issue"]/mods:number', namespaces=self.MODS_handle['MODS_ns'])[0].text

		# gen full identifier
		full_identifier = "DSJv" + volume + "i" + issue + identifier
		print "full identifier: %s " % full_identifier

		# generate PID
		PID = "wayne:%s" % (full_identifier)
		print "PID:",PID

		# get title for DSJ
		book_title = self.MODS_handle['MODS_element'].xpath('mods:titleInfo/mods:title',namespaces=self.MODS_handle['MODS_ns'])[0].text
		book_sub_title = self.MODS_handle['MODS_element'].xpath('mods:titleInfo/mods:subTitle',namespaces=self.MODS_handle['MODS_ns'])[0].text
		full_title = " ".join([book_title,book_sub_title])
		print "full title:",full_title

		# instantiate object with quick variables
		objMeta_primer = {
			"id":PID,
			"identifier":full_identifier,
			"label":full_title,
			"content_type":self.content_type,
			"image_filetype":"tif"
		}

		# instantiate ObjMeta object
		self.objMeta_handle = self.ObjMeta(**objMeta_primer)

		# iterate through SORTED binaries and create symlinks and write to objMeta		
		print "creating symlinks and writing to objMeta"
		print "looking in %s" % self.files_location

		# find DSJ folder by walking input
		identifier_suffix = identifier.split("DSJ")[1]
		for root,dirs,files in os.walk(self.files_location):
			for dir in dirs:
				if dir.endswith(identifier_suffix):
					d = "/".join([ root, dir ])
		print "target dir is %s" % d

		binary_files = [ binary for binary in os.listdir(d) if not binary.startswith('DSJ') ]
		binary_files.sort() #sort
		for ebook_binary in binary_files:

			# skip some undesirables
			if ebook_binary == ".DS_Store" or ebook_binary.endswith('bak') or ebook_binary == "Thumbs.db":
				continue

			# write symlink
			source = "/".join([ d, ebook_binary ])
			symlink = "/".join([ self.obj_dir, "datastreams", ebook_binary ])
			os.symlink(source, symlink)		

			# get mimetype of file
			filetype_hash = {
				'tif': ('image/tiff','IMAGE'),
				'jpg': ('image/jpeg','IMAGE'),
				'png': ('image/png','IMAGE'),
				'xml': ('text/xml','ALTOXML'),
				'html': ('text/html','HTML'),
				'htm': ('text/html','HTML'),
				'pdf': ('application/pdf','PDF')
			}
			filetype_tuple = filetype_hash[ebook_binary.split(".")[-1]] 		
			try:	
				page_num = ebook_binary.split(".")[0].split("_")[2].split("pg")[1].lstrip('0')
			except:
				page_num = ebook_binary.split(".")[0].split("_")[2].lstrip('0')

			# write to datastreams list		
			ds_dict = {
				"filename":ebook_binary,
				"ds_id":filetype_tuple[1]+"_"+page_num,
				"mimetype":filetype_tuple[0], # generate dynamically based on file extension
				"label":filetype_tuple[1]+"_"+page_num,
				"internal_relationships":{},
				'order':page_num			
			}
			self.objMeta_handle.datastreams.append(ds_dict)

			# set isRepresentedBy relationsihp
			'''
			This is problematic if missing the first page...
			'''
			if page_num == "1" and filetype_tuple[1] == 'IMAGE':
				self.objMeta_handle.isRepresentedBy = ds_dict['ds_id']


		################################################################		

		# write known relationships
		self.objMeta_handle.object_relationships = [				
			{
				"predicate": "info:fedora/fedora-system:def/relations-external#isMemberOfCollection",
				"object": "info:fedora/wayne:collection%s" % (self.collection_identifier)
			},
			{
				"predicate": "info:fedora/fedora-system:def/relations-external#isMemberOfCollection",
				"object": "info:fedora/wayne:collectionWSUebooks"
			},			
			{
				"predicate": "http://digital.library.wayne.edu/fedora/objects/wayne:WSUDOR-Fedora-Relations/datastreams/RELATIONS/content/isDiscoverable",
				"object": "info:fedora/True"
			},
			{
				"predicate": "http://digital.library.wayne.edu/fedora/objects/wayne:WSUDOR-Fedora-Relations/datastreams/RELATIONS/content/preferredContentModel",
				"object": "info:fedora/CM:%s" % (self.content_type)
			},
			{
				"predicate": "http://digital.library.wayne.edu/fedora/objects/wayne:WSUDOR-Fedora-Relations/datastreams/RELATIONS/content/hasSecurityPolicy",
				"object": "info:fedora/wayne:WSUDORSecurity-permit-apia-unrestricted"
			}		
		]

		# write to objMeta.json file 
		self.objMeta_handle.writeToFile("%s/objMeta.json" % (self.obj_dir))

		# make bag
		bag = bagit.make_bag(self.obj_dir, {
			'Collection PID' : "wayne:collection"+self.collection_identifier,
			'Object PID' : PID
		}, processes=1)

		# because ingestWorkspace() picks up from here, simply return bag location
		return self.obj_dir








