# Archivematica

import uuid, json, os
import bagit
from lxml import etree
import mimetypes
import metsrw

from WSUDOR_Manager import utilities
from inc import WSUDOR_bagger


# define required `BagClass` class
class BagClass(object):
	
		
	# class is expecting a healthy amount of input from `ingestWorkspace` script, and object row
	def __init__(self, object_row, ObjMeta, bag_root_dir, files_location, purge_bags):

		# hardcoded
		self.name = 'Archivematica_Hierarchical'  # human readable name, ideally matching filename, for this bag creating class
		self.content_type = False  # not required, but easy place to set the WSUDOR_ContentType

		# passed
		self.object_row = object_row  # handle for object mysql row in 'ingest_workspace_object'
		self.ObjMeta = ObjMeta  # ObjMeta class from ouroboros.models
		self.bag_root_dir = bag_root_dir  # path for depositing formed bags
		self.files_location = files_location  # location of files: they might be flat, nested, grouped, etc.
		
		# derived from object_row
		self.MODS = object_row.MODS  # MODS as XML string		
		self.struct_map = object_row.struct_map  # JSON representation of structMap section from METS file for this object
		self.object_title = (object_row.object_title[:100] + '..') if len(object_row.object_title) > 100 else object_row.object_title
		self.DMDID = object_row.DMDID  # object DMDID from METS, probabl identifier for file (but not required, might be in MODS)
		self.collection_identifier = object_row.job.collection_identifier  # collection signifier, likely suffix to 'wayne:collection[THIS]'
		self.object_type = object_row.object_type
		
		self.purge_bags = purge_bags

		# MODS_handle (parsed with etree)
		try:
			MODS_tree = etree.fromstring(self.MODS)
			ns = MODS_tree.nsmap
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


	# method to create bag
	def createBag(self):

		if not self.object_row.aem_enriched:
			# set identifier with filename
			self.full_identifier = self.collection_identifier+self.object_title.replace(".","_")

			# generate PID
			self.pid = "wayne:%s" % (self.full_identifier)
		else:			
			self.pid = self.object_row.pid
			self.full_identifier = self.pid.split("wayne:")[-1]

		# write MODS
		with open("%s/MODS.xml" % (self.obj_dir), "w") as fhand:
			fhand.write(self.MODS)

		# instantiate object with quick variables
		objMeta_primer = {
			"id": self.pid,
			"identifier": self.full_identifier,
			"label": self.object_title			
		}

		# Instantiate ObjMeta object
		self.objMeta_handle = self.ObjMeta(**objMeta_primer)
		
		# determine if physical or intellectual processing
		if self.object_type in ['Item','Directory']:
			result = self._createBag_physical()
		elif self.object_type in ['Intellectual']:
			result = self._createBag_intellectual()
		else:
			print "object_type %s not understood, cancelling" % self.object_type
			return False

		# write known relationships
		add_rels = [
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
		for rel in add_rels:
			self.objMeta_handle.object_relationships.append(rel)

		# update id and identifier from pid
		self.objMeta_handle.id = self.pid
		self.objMeta_handle.identifier = self.pid.split(":")[-1]

		# write to objMeta.json file
		self.objMeta_handle.writeToFile("%s/objMeta.json" % (self.obj_dir))

		# use WSUDOR bagger (NO MD5 CHECKSUMS)
		bag = WSUDOR_bagger.make_bag(self.obj_dir, {
			'Collection PID' : "wayne:collection"+self.collection_identifier,
			'Object PID' : self.pid
		})

		return self.obj_dir

	############################################################
	# Archivematica / Physical Object
	############################################################
	def _createBag_physical(self):

		# set namespaces
		ns = {
			'mets':'http://www.loc.gov/METS/',
			'premis':'info:lc/xmlns/premis-v2',
			'fits':'http://hul.harvard.edu/ois/xml/ns/fits/fits_output'
		}		

		# open mets		
		if not self.object_row.metsrw_parsed:
			print "##################### could not find parsed METS, parsing now"
			temp_filename = '/tmp/Ouroboros/%s.xml' % uuid.uuid4()
			with open(temp_filename, 'w') as fhand:
				fhand.write(self.object_row.job.ingest_metadata.encode('utf-8'))
			mets = metsrw.METSDocument.fromfile(temp_filename)
			os.remove(temp_filename)
		else:
			mets = self.object_row.metsrw_parsed

		# Build datastream dictionary
		'''
		Use label instead, as object_title might update?
		'''
		label = json.loads(self.struct_map)['ns0:div']['@LABEL']
		fs = fsByLabel(mets, label)
		# fs = fsByLabel(mets, self.object_title)
		print fs

		# determine parent
		try:
			parent_label = fs.parent.label.replace(".","_")
			parent_pid = "wayne:%s%s" % (self.collection_identifier, parent_label)
		except:
			print "Parent not found, setting collection PID"
			parent_pid = "wayne:collection%s" % (self.collection_identifier)

		# write parent
		self.objMeta_handle.object_relationships.append({
			"predicate": "http://digital.library.wayne.edu/fedora/objects/wayne:WSUDOR-Fedora-Relations/datastreams/RELATIONS/content/hasPhysicalParent",
			"object": "info:fedora/%s" % parent_pid
		})

		##########################################################################################
		# open WSU enrichment METS
		if self.object_row.aem_enriched:
			id_prefix = ''
			enrichment_METS = etree.fromstring(self.object_row.job.enrichment_metadata.encode('utf-8'))
			# get parent, else default to collection
			# try:
			print "Looking for %s to determine parent" % self.DMDID
			self_div = enrichment_METS.xpath('//mets:div[@DMDID="%s"]' % (self.DMDID), namespaces=ns)[0]
			parent_div = self_div.getparent()
			parent_DMDID = parent_div.attrib['DMDID']
			parent_pid = 'wayne:%s%s%s' % (id_prefix, self.collection_identifier, parent_DMDID.split("aem_prefix_")[-1])		
			print "Anticipating and setting hasParent pid: %s" % parent_pid
			self.objMeta_handle.object_relationships.append({
				"predicate": "http://digital.library.wayne.edu/fedora/objects/wayne:WSUDOR-Fedora-Relations/datastreams/RELATIONS/content/hasParent",
				"object": "info:fedora/%s" % parent_pid
			})

			# except:
			# 	print "Parent DMDID not found in enrichment METS"
			# 	if self.object_type == 'collection':
			# 		parent_pid = "wayne:collection%s" % (self.collection_identifier)
			# 		self.objMeta_handle.object_relationships.append({
			# 			"predicate": "http://digital.library.wayne.edu/fedora/objects/wayne:WSUDOR-Fedora-Relations/datastreams/RELATIONS/content/hasParent",
			# 			"object": "info:fedora/%s" % parent_pid
			# 		})	
			# 	else:
			# 		print "skipping parent PID"	
		##########################################################################################

		# for "Directory" types
		'''
		Has much less information than files, but does include parent
		'''
		if self.object_type == 'Directory':
			
			# set content types
			self.content_type = "WSUDOR_Container"
			self.objMeta_handle.content_type = self.content_type

			# set isRepresentedBy
			self.objMeta_handle.isRepresentedBy = False

		# for "Item" types
		'''
		With binary files, comes more information.  We can derive mimetype, and thus ContentType, from the fits metadata"
		'''
		if self.object_type == "Item":

			# remove file suffix from PID
			if not self.object_row.aem_enriched:			
				self.pid = "_".join(self.pid.split("_")[:-1])

			# determine mimetype

			# APPROACH #1 : use Archivematica analysis of file
			try:
				print "Attempting to derive mimetype from Archivematica output"
				# get amd section
				amd = fs.amdsecs[0]

				# get techMD as lxml element
				for sub in amd.subsections:
					if sub.id_string().startswith('techMD'):
						techMD = sub.serialize()

				# get identity section from fits tool, via premis event
				fits_identity = techMD.xpath('//premis:objectCharacteristicsExtension/fits:fits/fits:identification/fits:identity',namespaces=ns)[0]

				# get mimetype
				derived_mime_type = fits_identity.attrib['mimetype']
			
			# APPROACH #2 : fall back on file extension			
			except:
				print "Could not determine mime type of file from Archivematica output, attempting file extension"

				# get file extension
				file_ext = fs.path.split(".")[-1]				

				# use mimetypes library
				mimetypes.init()
				derived_mime_type = mimetypes.types_map[".%s" % file_ext]

			# Set content type here
			if derived_mime_type in utilities.mime_CM_hash:
				self.content_type = utilities.mime_CM_hash[derived_mime_type]
				self.objMeta_handle.content_type = self.content_type
			else:
				print "could not determine mimetype from fits, skipping"
				return False

			# write datastream
			self._makeDatastream(fs)


	############################################################
	# Intellectual Object
	############################################################
	def _createBag_intellectual(self):

		ns = {
			'mets':'http://www.loc.gov/METS/'
		}

		# set content type
		self.content_type = "WSUDOR_Container"
		self.objMeta_handle.content_type = self.content_type

		# set isRepresentedBy
		self.objMeta_handle.isRepresentedBy = False

		# if Intellectual object, change to anticipated PID
		if self.object_type == 'collection':
			id_prefix = 'collection'
		else:
			id_prefix = ''
		self.pid = 'wayne:%s%s%s' % (id_prefix, self.collection_identifier, self.DMDID.split("aem_prefix_")[-1])

		# open WSU enrichment METS
		enrichment_METS = etree.fromstring(self.object_row.job.enrichment_metadata.encode('utf-8'))

		# get parent, else default to collection
		try:
			self_div = enrichment_METS.xpath('//mets:div[@DMDID="%s"]' % (self.DMDID), namespaces=ns)[0]
			parent_div = self_div.getparent()
			parent_DMDID = parent_div.attrib['DMDID']
			parent_pid = 'wayne:%s%s%s' % (id_prefix, self.collection_identifier, parent_DMDID.split("aem_prefix_")[-1])		
			print "Anticipating parent pid: %s" % parent_pid
			self.objMeta_handle.object_relationships.append({
				"predicate": "http://digital.library.wayne.edu/fedora/objects/wayne:WSUDOR-Fedora-Relations/datastreams/RELATIONS/content/hasParent",
				"object": "info:fedora/%s" % parent_pid
			})

		except:
			print "Parent DMDID not found in enrichment METS"
			if self.object_type == 'collection':
				parent_pid = "wayne:collection%s" % (self.collection_identifier)
				self.objMeta_handle.object_relationships.append({
					"predicate": "http://digital.library.wayne.edu/fedora/objects/wayne:WSUDOR-Fedora-Relations/datastreams/RELATIONS/content/hasParent",
					"object": "info:fedora/%s" % parent_pid
				})	
			else:
				print "skipping parent PID"			


	# helper function to build datastream directory
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

		# fix ds_id and label
		ds_id = "DS%s" % ds_id
		label = "DS%s" % label

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


# HELPER FUNCTIONS (TO MOVE)
def fsByLabel(mets,label):
	for fs in mets.all_files():
		if fs.label == label:
			return fs




