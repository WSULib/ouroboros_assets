# template for bag creation class 

import uuid, json, os
import bagit


# define required `BagClass` class
class BagClass(object):
	
		
	# class is expecting a healthy amount of input from `ingestWorkspace` script, and object row
	def __init__(self, ObjMeta, bag_root_dir, files_location, MODS, struct_map, object_title, DMDID, collection_identifier):

		self.name = 'bag_class_template' # human readable name, ideally matching filename, for this bag creating class 
		self.content_type = 'WSUDOR_BagTemplate' # not required, but easy place to set the WSUDOR_ContentType 
		self.ObjMeta = ObjMeta # ObjMeta class from ouroboros.models
		self.bag_root_dir = bag_root_dir # path for depositing formed bags
		self.files_location = files_location # location of files: they might be flat, nested, grouped, etc.
		self.MODS = MODS # MODS as XML string
		self.struct_map = struct_map # JSON representation of structMap section from METS file for this object
		self.object_title = object_title
		self.DMDID = DMDID # object DMDID from METS, probabl identifier for file (but not required, might be in MODS)
		self.collection_identifier = collection_identifier # collection signifier, likely suffix to 'wayne:collection[THIS]'

		# generate
		self.obj_dir = "/".join( [bag_root_dir, str(uuid.uuid4())] ) # UUID based hash directory for bag
		if not os.path.exists(self.obj_dir):
			os.mkdir(self.obj_dir)

	def createBag(self):

		'''
		Function to create bag given inputs.  Most extensive and complex part of this class.
		'''

		# put together bag here 

		# set identifier
		full_identifier = self.DMDID
		print full_identifier

		# generate PID
		PID = "wayne:%s" % (full_identifier)

		# write MODS
		with open("%s/MODS.xml" % (self.obj_dir), "w") as fhand:
			fhand.write(self.MODS)

		# set label (might be from MODS, or object title)
	
		# instantiate object with quick variables
		objMeta_primer = {
			"id":"wayne:"+full_identifier,
			"identifier":full_identifier,
			"label":self.object_title,
			"content_type":self.content_type
		}			

		# instantiate ObjMeta object
		om_handle = self.ObjMeta(**objMeta_primer)

		# write known relationships
		om_handle.object_relationships = [				
			{
				"predicate": "info:fedora/fedora-system:def/relations-external#isMemberOfCollection",
				"object": "info:fedora/wayne:collection%s" % (self.collection_identifier)
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
		om_handle.writeToFile("%s/objMeta.json" % (self.obj_dir))

		# make bag
		bag = bagit.make_bag(self.obj_dir, {
			'Collection PID' : "wayne:collection"+self.collection_identifier,
			'Object PID' : PID
		}, processes=1)

		return self.obj_dir








