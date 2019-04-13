import boto3
from boto3.dynamodb.conditions import Key

def create_fixed_box_image_cropper(box, box_normalized=False):
    '''
    Create a function which take an image to produce an array of cropped images, according to the box supplied here
    :param boxes: box for cropping
    :return: function mapping from image to a list of images
    '''
    cropper_fn = create_image_cropper(box_normalized)
    return lambda image_arr: cropper_fn(image_arr=image_arr,boxes=[box])[0]



def create_image_cropper(box_normalized = False):
    '''
    create a function which will take an array of boxes and an image to produce an array of cropped images
    :return: function mapping from image + list of boxes to list of images
    '''
    def image_cropping_with_boxes(image_arr, boxes):
        rt_arr = []
        for box in boxes:
            if box_normalized:
                unnorm_boxes = unnormalize_boxes([box],image_arr)
                ymin, xmin, ymax, xmax = unnorm_boxes[0]
            else:
                ymin, xmin, ymax, xmax = box
            cur_crop = image_arr[ymin:ymax, xmin:xmax, :]
            rt_arr.append(cur_crop)
        return rt_arr
    return image_cropping_with_boxes



def unnormalize_boxes(normalized_boxes, frame):
    unnormalized_boxes = []
    frame_w = frame.shape[-2]
    frame_h = frame.shape[-3]

    for good_box_normalized in normalized_boxes:
        ymin, xmin, ymax, xmax = good_box_normalized
        unnormalized_boxes.append([int(ymin * frame_h), int(xmin * frame_w),
                                   int(ymax * frame_h), int(xmax * frame_w)])
    return unnormalized_boxes




# Constant for rendering roi
left_offset = 380
left_bottom_offset = 450
left_side = 304//2*3


def get_left_ymin_xmin():
    ymin = 2160 -left_bottom_offset- left_side
    xmin = left_offset
    return ymin, xmin
def get_left_ymax_xmax():
    return 2160-left_bottom_offset, left_offset + left_side


def get_left_corner_cropper():
    ymin,xmin = get_left_ymin_xmin()
    ymax,xmax = get_left_ymax_xmax()
    print(ymin,xmin,ymax,xmax)
    image_square_crop_fn = create_fixed_box_image_cropper(
        box=[ymin,xmin ,ymax,xmax ]
    )
    return image_square_crop_fn


# dynamodb stuff

class DynamodbComm:
    def __init__(self):
        self.cur_dynamodb = boto3.resource('dynamodb')

    def query_table_index_top_sorted_key(self, table_name, index_name, part_key_name, part_key_val, min_not_max,
                                         sorted_key_name=None, sorted_key_low_incl=None, sorted_key_high_incl=None):
        table = self.cur_dynamodb.Table(table_name)
        key_cond = Key(part_key_name).eq(part_key_val)
        if sorted_key_name is not None:
            if sorted_key_low_incl is not None:
                key_cond = key_cond & Key(sorted_key_name).gte(sorted_key_low_incl)
            if sorted_key_high_incl is not None:
                key_cond = key_cond & Key(sorted_key_name).lte(sorted_key_high_incl)

        if index_name is not None:
            response = table.query(
                IndexName=index_name,
                KeyConditionExpression=key_cond,
                ScanIndexForward=min_not_max,
                Limit=1
            )
            return response['Items']
        else:
            response = table.query(
                KeyConditionExpression=key_cond,
                ScanIndexForward=min_not_max,
                Limit=1
            )
            return response['Items']

    def query_table_index(self, table_name, index_name, part_key_name, part_key_val, sorted_key_name=None,
                          sorted_key_val=None, sorted_key_low_incl=None, sorted_key_high_incl=None):
        table = self.cur_dynamodb.Table(table_name)
        key_cond = Key(part_key_name).eq(part_key_val)
        if sorted_key_name is not None:
            if sorted_key_val is not None:
                key_cond = key_cond & Key(sorted_key_name).eq(sorted_key_val)
            elif sorted_key_low_incl is not None and sorted_key_high_incl is not None:
                key_cond = key_cond & Key(sorted_key_name).between(sorted_key_low_incl, sorted_key_high_incl)
            elif sorted_key_low_incl is not None:
                key_cond = key_cond & Key(sorted_key_name).gte(sorted_key_low_incl)
            elif sorted_key_high_incl is not None:
                key_cond = key_cond & Key(sorted_key_name).lte(sorted_key_high_incl)
            else:
                raise Exception("Sort key name given but values are ill-specified")

        if index_name is not None:
            response = table.query(
                IndexName=index_name,
                KeyConditionExpression=key_cond
            )
            data = response['Items']

            while response.get('LastEvaluatedKey'):
                response = table.query(
                    IndexName=index_name,
                    KeyConditionExpression=key_cond,
                    ExclusiveStartKey=response['LastEvaluatedKey'])
                data.extend(response['Items'])

        else:
            response = table.query(
                KeyConditionExpression=key_cond
            )
            data = response['Items']
            while response.get('LastEvaluatedKey'):
                response = table.query(
                    KeyConditionExpression=key_cond,
                    ExclusiveStartKey=response['LastEvaluatedKey'])
                data.extend(response['Items'])
        return data

    def put_item_in_table(self, table_name, item_to_upload):
        table = self.cur_dynamodb.Table(table_name)
        response = table.put_item(Item=item_to_upload)
        return response

    def get_item_in_table(self, table_name, part_key_name, part_key_val, sorted_key_name=None, sorted_key_val=None):
        table = self.cur_dynamodb.Table(table_name)
        item_keys = dict()
        item_keys[part_key_name] = part_key_val
        if sorted_key_name:
            item_keys[sorted_key_name] = sorted_key_val
        response = table.get_item(
            Key=item_keys
        )
        if 'Item' in response:
            item = response['Item']
            return item
        else:
            return None

    def delete_item_from_table(self, table_name, part_key_name, part_key_val, sorted_key_name=None,
                               sorted_key_val=None):
        table = self.cur_dynamodb.Table(table_name)
        item_keys = dict()
        item_keys[part_key_name] = part_key_val
        if sorted_key_name:
            item_keys[sorted_key_name] = sorted_key_val
        table.delete_item(
            Key=item_keys
        )

