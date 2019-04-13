from util import get_left_corner_cropper, DynamodbComm, unnormalize_boxes, get_left_ymin_xmin, get_left_ymax_xmax
import cv2
import copy

in_video = cv2.VideoCapture('./test_data/in.mp4')
resize_factor = 0.5
out_video = cv2.VideoWriter('./out.avi', cv2.VideoWriter_fourcc('X', 'V', 'I', 'D'), 24, (int(in_video.get(3)*resize_factor),int(in_video.get(4)*resize_factor)))

ddb = DynamodbComm()
src_id = 'cons_left_final'

RED = (0,0,255)
BLUE = (255,0,0)
GREEN = (255,255,0)
font = cv2.FONT_HERSHEY_SIMPLEX



left_stich_x_offset = 50
left_stich_y_offset = 50


#
def extract_detection_info_from_db(fcount):
    item = ddb.get_item_in_table('cox_prelim_trial', 'src_id', src_id, 'timestamp', fcount)
    people_norm_boxes = [[float(coordstr) for coordstr in event['box']] for event in item['events']] \
        if item is not None else None
    extra_infos = [event['extra'] for event in item['events']] \
        if item is not None else None

    return people_norm_boxes, extra_infos

def render_roi_with_detection(roi,fcount):
    norm_boxes, extra_infos = extract_detection_info_from_db(fcount)
    if norm_boxes is None or extra_infos is None:
        return roi
    rt_roi = copy.deepcopy(roi)
    unnorm_boxes = unnormalize_boxes(norm_boxes,roi)
    for unnorm_box, hat_status in zip(unnorm_boxes,extra_infos):
        if hat_status == '"hat_on"':
            color = BLUE
            text = 'SAFE'
        elif hat_status == '"hat_off"':
            color = RED
            text = 'UNSAFE'
        else:
            raise RuntimeError('Unexpected person tag')
        ymin,xmin,ymax,xmax = unnorm_box
        cv2.rectangle(rt_roi, (xmin, ymin), (xmax, ymax), color, int(2 / resize_factor))
        cv2.putText(rt_roi, text, (xmin, ymin - 5), font, 0.5 / resize_factor, color)
    return rt_roi

def overlay_roi_on_frame(rendered_roi, frame):
    frame[left_stich_y_offset:left_stich_y_offset + rendered_roi.shape[0], \
        left_stich_x_offset:left_stich_x_offset + rendered_roi.shape[1], :] = rendered_roi
    return frame


def annotate_roi(frame):
    left_ymin, left_xmin = get_left_ymin_xmin()
    left_ymax, left_xmax = get_left_ymax_xmax()
    cv2.rectangle(frame, (left_xmin, left_ymin), (left_xmax, left_ymax), GREEN, 8)
    cv2.putText(frame, 'User-specified RoI', (left_xmin - 30, left_ymin - 30), font, 2, GREEN, 4,
                cv2.LINE_AA)

def render_frames():
    roi_cropper = get_left_corner_cropper()
    roi_render_start = 8
    fcount = 0
    while True:
        valid, frame = in_video.read()
        if not valid:
            break
        annotate_roi(frame)
        if fcount >=roi_render_start:
            roi = roi_cropper(frame)
            rendered_roi = render_roi_with_detection(roi,fcount)
        else:
            rendered_roi = None
        frame = cv2.resize(frame, (0, 0), fx=resize_factor, fy=resize_factor)
        if rendered_roi is not None:
            frame = overlay_roi_on_frame(rendered_roi, frame)
        cv2.imshow('cur_render', frame)
        cv2.waitKey(10)
        fcount += 1
        out_video.write(frame)

if __name__ =='__main__':
    render_frames()

