# Archivematica

import uuid, json, os
import bagit
from lxml import etree
import mimetypes
import metsrw


# define required `BagClass` class
class BagClass(object):
	
		
	# class is expecting a healthy amount of input from `ingestWorkspace` script, and object row
	def __init__(self, o, ObjMeta, bag_root_dir, files_location, purge_bags):

		# hardcoded
		self.name = 'Archivematica_Hierarchical'  # human readable name, ideally matching filename, for this bag creating class
		self.content_type = 'WSUDOR_HierarchicalFiles'  # not required, but easy place to set the WSUDOR_ContentType

		# passed
		self.object_row = o  # handle for object mysql row in 'ingest_workspace_object'
		self.ObjMeta = ObjMeta  # ObjMeta class from ouroboros.models
		self.bag_root_dir = bag_root_dir  # path for depositing formed bags
		self.files_location = files_location  # location of files: they might be flat, nested, grouped, etc.
		
		# derived from object_row
		self.MODS = o.MODS  # MODS as XML string		
		self.struct_map = o.struct_map  # JSON representation of structMap section from METS file for this object
		self.object_title = o.object_title
		self.DMDID = o.DMDID  # object DMDID from METS, probabl identifier for file (but not required, might be in MODS)
		self.collection_identifier = o.collection_identifier  # collection signifier, likely suffix to 'wayne:collection[THIS]'
		
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


	def _makeDatastream(self, fs):

		# Identify datastreams folder
		datastreams_dir = self.obj_dir + "/datastreams"

		filename = fs.path.split("/")[-1]
		label = fs.label
		'''
		Need to resolve issue of order...
		'''
		order = 1 

		# get extension, ds_id
		mimetypes.init()
		ds_id, ext = os.path.splitext(filename)

		# create datastream dictionary
		ds_dict = {
			"filename": filename,
			"ds_id": ds_id,
			"mimetype": mimetypes.types_map[ext],
			"label": label,
			"internal_relationships": {},
			'order': order
		}

		self.objMeta_handle.datastreams.append(ds_dict)

		# make symlinks to datastreams on disk
		bag_location = datastreams_dir + "/" + filename

		# determine remote_location by parsing filename
		remote_location = self.files_location + "/" + fs.path
		os.symlink(remote_location, bag_location)

		# Set the representative image for the object
		if int(order) == 1:
			self.objMeta_handle.isRepresentedBy = ds_id

	def createBag(self):

		# set identifier
		self.full_identifier = self.collection_identifier+self.DMDID
		print self.full_identifier

		# generate PID
		self.pid = "wayne:%s" % (self.full_identifier)

		# write MODS
		with open("%s/MODS.xml" % (self.obj_dir), "w") as fhand:
			fhand.write(self.MODS)

		# instantiate object with quick variables
		objMeta_primer = {
			"id": self.pid,
			"identifier": self.full_identifier,
			"label": self.object_title,
			"content_type": self.content_type
		}

		# Instantiate ObjMeta object
		self.objMeta_handle = self.ObjMeta(**objMeta_primer)

		# open mets
		'''
		Obviously, want to avoid this decode / encode thing here...
		'''
		mets = metsrw.METSDocument.fromtree( etree.fromstring(self.object_row.job.ingest_metadata.decode('utf-8').encode('ascii')) )
		print mets.all_files()

		# Build datstream dictionary
		fs = mets.get_file(self.DMDID)
		'''
		at this point, we can know the mimetype of this file, and trigger the correct WSUDOR_ContentType
			- assumes object is "simple", only one file (maybe the best way... look at converting others)
			- trigger CM based on mimetype as determined by Archivematica

		for "container" / directory front-end view, we'll see the thumbnails of children objects
		'''

		self._makeDatastream(fs)

		# write known relationships
		self.objMeta_handle.object_relationships = [
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
		bagit.make_bag(self.obj_dir, {
			'Collection PID': "wayne:collection"+self.collection_identifier,
			'Object PID': self.pid
		}, processes=1)

		return self.obj_dir








