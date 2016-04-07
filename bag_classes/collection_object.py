# Collection Object class

import uuid, json, os
import bagit
from lxml import etree
import mimetypes


# define required `BagClass` class
class BagClass(object):
	
		
	# class is expecting a healthy amount of input from `ingestWorkspace` script, and object row
	def __init__(self, object_row, ObjMeta, bag_root_dir, files_location, MODS, MODS_handle, struct_map, object_title, DMDID, collection_identifier, purge_bags):

		# hardcoded
		self.name = 'Collection' # human readable name, ideally matching filename, for this bag creating class 
		self.content_type = 'WSUDOR_Collection' # not required, but easy place to set the WSUDOR_ContentType		

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
		self.full_identifier = self.DMDID
		print self.full_identifier

		# generate PID
		self.pid = "wayne:collection%s" % (self.full_identifier)

		# write MODS
		with open("%s/MODS.xml" % (self.obj_dir), "w") as fhand:
			fhand.write(self.MODS)		
	
		# instantiate object with quick variables
		objMeta_primer = {
			"id":self.pid,
			"identifier":self.full_identifier,
			"label":self.object_title,
			"content_type":self.content_type
		}

		# Instantiate ObjMeta object
		self.objMeta_handle = self.ObjMeta(**objMeta_primer)

		################################################################
		# set Collection Art
		
		# Identify datastreams folder
		datastreams_dir = self.obj_dir + "/datastreams"

		# collection art file
		print "Looking in: %s" % self.files_location
		art_files = [filename for filename in os.listdir(self.files_location) if filename.startswith("COLLECTIONART")]
		if len(art_files) == 1:
			filename = art_files[0]

			label = "Collection Art"
			order = 1

			# get extension, ds_id
			mimetypes.init()
			ds_id, ext = os.path.splitext(filename)

			# create datastream dictionary
			ds_dict = {
				"filename": filename,
				"ds_id": 'COLLECTIONART',
				"mimetype": mimetypes.types_map[ext],
				"label": label,
				"internal_relationships": {},
				'order': order
			}

			self.objMeta_handle.datastreams.append(ds_dict)

			# make symlinks to datastreams on disk
			bag_location = datastreams_dir + "/" + filename

			# determine remote_location by parsing filename
			filename_parts = filename.split("_")
			remote_location = "/".join([ self.files_location, filename ])
			os.symlink(remote_location, bag_location)
			
			# set as representative datastream
			self.objMeta_handle.isRepresentedBy = 'COLLECTIONART'
		
		else:
			print "Could not locate Collection Art, skipping."
		

		################################################################		

		# write known relationships
		self.objMeta_handle.object_relationships = [
			{
				"predicate": "http://digital.library.wayne.edu/fedora/objects/wayne:WSUDOR-Fedora-Relations/datastreams/RELATIONS/content/isDiscoverable",
				"object": "info:fedora/True"
			},
			{
				"predicate": "http://digital.library.wayne.edu/fedora/objects/wayne:WSUDOR-Fedora-Relations/datastreams/RELATIONS/content/preferredContentModel",
				"object": "info:fedora/CM:%s" % (self.content_type.split("_")[1])
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
			'Collection PID' : self.pid,
			'Object PID' : self.pid
		}, processes=1)


		return self.obj_dir







