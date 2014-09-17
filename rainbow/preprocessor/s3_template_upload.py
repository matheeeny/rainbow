from base import PreprocessorBase
from preprocessor_exceptions import PreprocessorBaseException
from rainbow.templates import TemplateLoader

import boto
from boto.s3.key import Key
import os.path
import json


class S3UploadException(PreprocessorBaseException):
    pass

class S3TemplateUpload:

    def __init__(self, preprocessor, args):
        if len(args) != 2:
            raise S3UploadException("S3TemplateUpload requires two arguments")

        self.preprocessor = preprocessor
        self.s3_conn = boto.connect_s3()
        self.s3_bucket = self.validate_s3_bucket(args[0])
        self.s3_key = self.validate_s3_key(args[0])
        self.local_template = self.validate_local_template(args[1])

    def validate_template(self):
        cfn = boto.cloudformation.connect_to_region(self.preprocessor.region or Cloudformation.default_region)
        template_json = json.dumps(self.local_template)
        try:
            cfn.validate_template(template_json)
        except Exception as e:
            raise S3UploadException("The provided template file failed validation", e)

    def upload(self):
        k = Key(self.s3_bucket)
        k.key = self.s3_key
        try:
          k.set_contents_from_string(json.dumps(self.local_template))
        except Exception as e:
          raise S3UploadException("Unable to upload template to s3", e)

        return "https://s3.amazonaws.com/%s/%s" % (self.s3_bucket.name, self.s3_key)

    def validate_s3_bucket(self, s3_location):
        split = s3_location.split('/', 1)
        if len(split) != 2:
            self.raise_illegal_s3_loc()
        try:
            return self.s3_conn.get_bucket(split[0])
        except S3ResponseError:
            raise S3UploadException("Unable to connect to the bucket " + bucket)

    def validate_s3_key(self, s3_location):
        split = s3_location.split('/', 1)
        if len(split) != 2:
            self.raise_illegal_s3_loc()
        if not split[1]:
            self.raise_illegal_s3_loc()
        return split[1]

    def validate_local_template(self, path):
        if not os.path.isfile(path):
            raise S3UploadException("No file found at " + path)
        template = TemplateLoader.load_templates([path])
        return self.preprocessor.process(template)

    def raise_illegal_s3_loc(self):
        raise S3UploadException("Please specify a path to an s3 location in the form "
                                "'mybucket.mycorp.com/path/to/a/file.template'")

@PreprocessorBase.expose('S3TemplateUpload')
def s3_template_upload(preprocessor, args):
    """
    Rb::S3Upload
        Upload a template to the provided bucket/key and return the http url
        This is particularly useful when dealing with Cloudformation stacks as Cloudformation
        resources (nested stacks).  If the provided file is yaml, it will be converted to JSON.
        Before uploading, the provided template will be validated via the boto cloudformation API
    Example usage:
        {'Rb::S3TemplateUpload': ['cfn-templates.mycorp.com/substacks/my-template.template', 'my-template.yaml']}
        {'Rb::S3TemplateUpload': ['cfn-templates.mycorp.com/substacks/my-template.template', 'my-template.json']}

    :param preprocessor: Preprocessor instance processing the function
    :type preprocessor: Preprocessor
    :param args: list of arguments including an s3 location and a path to a local json/yaml template file
    :type args: list
    :rtype: str
    """

    uploader = S3TemplateUpload(preprocessor, args)
    uploader.validate_template()
    return uploader.upload()
