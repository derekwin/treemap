from copy import deepcopy
from enum import Enum
import random
import uuid
import math

class Region(Enum):
    Asia = 0
    Europe = 1
    NorthAmerica = 2
    SouthAmerica = 3
    
class Zone(Enum):
    Zone1 = 0
    Zone2 = 1
    Zone3 = 2
    Zone4 = 3
    
class SubZone(Enum):
    subZone1 = 0
    subZone2 = 1
    subZone3 = 2
    subZone4 = 3

class Endpoint:
    def __init__(self, r: Region, z: Zone, s: SubZone, name=None) -> None:
        self.uid = self.generate_uid()
        self.name = name
        self.region = r
        self.zone = z
        self.subzone = s
    
    # 调用uuid库生成唯一的uid
    def generate_uid(self):
        return uuid.uuid4()

class Node():
    def __init__(self) -> None:
        self.sons = {}    # 子树节点   key zone/subzone/uid, value Node
    
    def sons_k_list(self):
        return list(self.sons.keys())
    
    def sons_v_list(self):
        return list(self.sons.values())

def build_forest(forest: dict, w: Endpoint):
    if w.region not in forest.keys():
        forest[w.region] = Node()
    
    if w.zone not in forest[w.region].sons.keys():
        forest[w.region].sons[w.zone] = Node()
    
    if w.subzone not in forest[w.region].sons[w.zone].sons.keys():
        forest[w.region].sons[w.zone].sons[w.subzone] = Node()
    
    forest[w.region].sons[w.zone].sons[w.subzone].sons[w.uid] = w.uid  # 叶子节点 key uid，value uid

def random_choose(nodes: list):
    # print(nodes)
    node = random.choice(nodes)
    # print(node)
    while type(node) == Node:
        node = node.sons[random.choice(node.sons_v_list())]  # 如果是Node类型，则从子树中随机选择一个节点
        # print(node)
        # 如果不是Node类型，说明已经选择到了叶子节点，即随机选出了uid
    return node

def load_balance_by_forest(testWl: Endpoint, forest: dict):
    if testWl.region not in forest.keys():  # workload不属于任何树，则从所有森林里面随机选择
        return random_choose(list(forest.values()))
    
    root = forest[testWl.region]  # 匹配到了region，（树存在，说明至少有一个节点存在）
    if testWl.zone not in root.sons_k_list():  # workload的zone在子树中不存在，说明没有同region同zone的节点，则从兄弟节点中随机选择一个节点，满足次要最近
        return random_choose(root.sons_v_list())
    
    subroot = root.sons[testWl.zone]  # 匹配到了zone
    if testWl.subzone not in subroot.sons_k_list():  # workload的subzone在zone子树中不存在，说明没有同region同zone的节点，则从兄弟节点中随机选择一个节点，满足次要最近
        return random_choose(subroot.sons_v_list())
    
    # 注意这里也要用values
    nodes = subroot.sons_v_list()  # 匹配到了subzone, 这个里面存的都是匹配度最高的节点, 此时的sons_list()就是uid的list
    # print(nodes, type(nodes[0])==Node)
    return random_choose(nodes)
    

class TreeNode:
    def __init__(self, value=-1, left=None, right=None):
        self.value = value  # region/zone/subzone
        self.left = left
        self.right = right

def build_bin_tree(root: TreeNode, w: Endpoint, findDict: dict):
    
    root_ptr = root
    
    fdkey = [-1, -1, -1]
    
    if root_ptr.value == -1:
        # 完全初始化
        root_ptr.value = w.region
        # 此时必然也不存在相应的左子树，所以新建左子树
        root_ptr.left = TreeNode(w.zone)
        root_ptr = root_ptr.left
        root_ptr.left = TreeNode(w.subzone)
        fdkey = [w.region, w.zone, w.subzone]
        
        fdkey = "".join([str(i.value) for i in fdkey])
        if fdkey not in findDict:
            findDict[fdkey]=[]
        findDict[fdkey].append(w)
        return 
    
    if root_ptr.value == w.region: # 根就是，直接入左树
        fdkey[0] = w.region # to 跳转左树
            
    # 在右树根中查找，是否有当前wl的region
    while root_ptr.right != None:
        root_ptr = root_ptr.right # 继续查找 to 新建右树
        if root_ptr.value == w.region:
            # 找到了, 则进入左子树
            fdkey[0] = w.region # to 跳转左树
            break
    
    # 新建右树
    if fdkey[0] == -1: # 说明没找到，则新建右子树
        root_ptr.right = TreeNode(w.region)
        # 此时必然也不存在相应的左子树，所以新建左子树
        root_ptr = root_ptr.right
        root_ptr.left = TreeNode(w.zone)
        root_ptr = root_ptr.left
        root_ptr.left = TreeNode(w.subzone)
        fdkey = [w.region, w.zone, w.subzone]
        
        fdkey = "".join([str(i.value) for i in fdkey])
        if fdkey not in findDict:
            findDict[fdkey]=[]
        findDict[fdkey].append(w)
        return
    
    ####################################
    # 跳转左树 
    # 该树的特点，只要有左树，则左树必然存在，不需要考虑新建的情况
    root_ptr = root_ptr.left
    
    # 找到了region，在左子树查下一级信息
    if root_ptr.value == w.zone: # 根就是，直接再次入左树
        fdkey[1] = w.zone # to 跳转左树
            
    # 在右树根中查找，是否有当前wl的region
    while root_ptr.right != None:
        root_ptr = root_ptr.right # 继续查找 to 新建右树
        if root_ptr.value == w.zone:
            # 找到了, 则进入左子树
            fdkey[1] = w.zone # to 跳转左树
            break
    
    # 新建右树
    if fdkey[1] == -1: # 说明没找到，则新建右子树
        root_ptr.right = TreeNode(w.zone)
        # 此时必然也不存在相应的左子树，所以新建左子树
        root_ptr = root_ptr.right
        root_ptr.left = TreeNode(w.subzone)
        fdkey = [w.region, w.zone, w.subzone]
        
        fdkey = "".join([str(i.value) for i in fdkey])
        if fdkey not in findDict:
            findDict[fdkey]=[]
        findDict[fdkey].append(w)
        return
    
    ####################################
    # 跳转左树 
    root_ptr = root_ptr.left
    # 找到了zone，在左子树查下一级信息
    if root_ptr.value == w.subzone:
        fdkey[2] = w.subzone # 查找完毕
            
    # 在右树根中查找，是否有当前wl的region
    while root_ptr.right != None:
        root_ptr = root_ptr.right # 继续查找 to 新建右树
        if root_ptr.value == w.subzone:
            # 找到了, 查找完毕
            fdkey[2] = w.subzone # to 跳转左树
            break
    
    # 新建右树
    if fdkey[2] == -1: # 没找到，则新建右子树
        root_ptr.right = TreeNode(w.subzone)
        # 此时必然也不存在相应的左子树，所以新建左子树
        fdkey = [w.region, w.zone, w.subzone]
        
        fdkey = "".join([str(i.value) for i in fdkey])
        if fdkey not in findDict:
            findDict[fdkey]=[]
        findDict[fdkey].append(w)
        return

    fdkey = "".join([str(i.value) for i in fdkey])
    findDict[fdkey].append(w)
    return

def delete_bin_tree(root: TreeNode, w: Endpoint, findDict: dict):  # 用带父节点指针的树会简单很多
    # 删除节点
    
    ## 如果有 大于1个 则删除uid，不删除子树
    fdkey = [w.region, w.zone, w.subzone]
    fdKey = "".join([str(i.value) for i in fdkey])
    
    findDict[fdKey].remove(w)
    
    def find_node_path(): # 找到该分支对应的每个节点，和节点的父节点
        region_node = None
        region_node_father = None
        zone_node = None
        zone_node_father = None
        subzone_node = None
        subzone_node_father = None
        tmp_root = root
        # print(tmp_root.value.value, w.region.value)
        if tmp_root.value.value == w.region.value:
            region_node = tmp_root  # 注意后续的改变，并不会影响region_node
        else:
            while tmp_root.right!=None:
                region_node_father = tmp_root
                tmp_root = tmp_root.right
                # print(">")
                if tmp_root.value.value == w.region.value:
                    region_node = tmp_root  # 
                    break
        
        zone_node_father = tmp_root
        tmp_root = tmp_root.left # to 跳转左树
        # print("<")
        if tmp_root.value.value == w.zone.value:
            zone_node = tmp_root  # 注意后续的改变，并不会影响region_node
        else:
            while tmp_root.right!=None:
                zone_node_father = tmp_root
                tmp_root = tmp_root.right
                # print(">")
                if tmp_root.value.value == w.zone.value:
                    zone_node = tmp_root  # 
                    break
        
        subzone_node_father = tmp_root
        tmp_root = tmp_root.left # to 跳转左树
        # print("<")
        if tmp_root.value.value == w.subzone.value:
            subzone_node = tmp_root  # 注意后续的改变，并不会影响region_node
        else:
            while tmp_root.right!=None:
                subzone_node_father = tmp_root
                tmp_root = tmp_root.right
                # print(">")
                if tmp_root.value.value == w.subzone.value:
                    subzone_node = tmp_root  # 
                    break
        return region_node, region_node_father, zone_node, zone_node_father, subzone_node, subzone_node_father
    
    if len(findDict[fdKey]) < 1:
        # 删除该子树
        region_node, region_node_father, zone_node, zone_node_father, subzone_node, subzone_node_father = find_node_path()
        result = find_node_path()
        # for i in result:
        #     if i != None:
        #         print(i.value)
        #     else:
        #         print("None")
        region_node, region_node_father, zone_node, zone_node_father, subzone_node, subzone_node_father = result
        # 从叶往根删
        subzone_node_father.left = None # 先把subzone删掉
        #然后删掉subzone的父节点zone_node
        if zone_node.left == None:
            zone_node_father.right = zone_node.right
        #然后 如果该zone右子没有后续了，才删掉zone的父节点region_node
        if region_node_father!=None and region_node.left.left == None: # 刚好上面删除完，删的是唯一左子树，则直接删掉region_node
            region_node_father.right = region_node.right
        if region_node_father==None:
            root = region_node.right
        # 否则说明有其他路径，不能删
    
def print_tree(treeRoot):
    if treeRoot == None:
        return
    print(treeRoot.value)
    print_tree(treeRoot.left)
    print_tree(treeRoot.right)

def dump_bintree_array(treeRoot):
    # 把非完全二叉树转为完全二叉树
    result = []
    # queue = [(treeRoot, 0)]  # 使用元组 (node, index) 存储节点及其索引
    virtual_queue = [(treeRoot, 0)]  # 使用元组 (node, index) 存储节点及其索引, 会增加虚拟节点，用于补全二叉树
    index = 0
    
    true_node_count = 1  # 记录virtual queue中真实node的数量，当为0的时候，结束遍历
    
    while virtual_queue: # 以带有虚拟节点的作为队列，不断往里放
        if true_node_count == 0:
            break  # 结束遍历
        node, idx = virtual_queue.pop(0)
        if node.value != -1: # 退出的是真实节点，则真实节点数量减一
            true_node_count-=1
        # print("pop:", node.value, node, "true count:", true_node_count)
        if node: # node存在
            if node.value == -1:
                result.append(None) # 虚节点
            else:
                result.append(node.value)
            if node.left == None: # 左节点不存在，则插入一个虚拟节点
                virtual_queue.append((TreeNode(), 2 * idx + 1))
                # print("add left v:", "true count:", true_node_count)
            else:
                virtual_queue.append((node.left, 2 * idx + 1)) # 否则是真实节点
                true_node_count+=1
                # print("add left:", node.left, "true count:", true_node_count)
            if node.right == None: # 右节点不存在，则插入一个虚拟节点
                virtual_queue.append((TreeNode(), 2 * idx + 2))
                # print("add right v:", "true count:", true_node_count)
            else:
                virtual_queue.append((node.right, 2 * idx + 2)) # 否则是真实节点
                true_node_count+=1
                # print("add right:", node.right, "true count:", true_node_count)
        else: # 节点不存在，或者是虚节点
            result.append(None)
    
    # 移除末尾的 None 值
    while result and result[-1] is None:
        result.pop()
    
    return result

def loadbalcance(wl: Endpoint, treeList: list, findDict: dict):
    
    choose_region = -1
    choose_zone = -1
    choose_subzone = -1
    
    def find_right_or_random_choose(match, from_index, treeList):
        record = []
        index = from_index
        region = -1
        # print(index, len(treeList))
        while index < len(treeList):
            # print(index, treeList[index])
            if treeList[index] != "-":
                record.append((index, treeList[index]))
            # print("match:", match, treeList[index])
            if treeList[index] == match:
                region = match
                break
            index = 2*index + 2
        if region == -1: # 没有匹配到
            c = random.choice(record)
            index = c[0]
            region = c[1]
        return index, region

    choose_index, choose_region = find_right_or_random_choose(wl.region.value, 0, treeList)
    # print("find zone")
    choose_index, choose_zone = find_right_or_random_choose(wl.zone.value, 2*choose_index+1, treeList)
    # print("find subzone")
    choose_index, choose_subzone = find_right_or_random_choose(wl.subzone.value, 2*choose_index+1, treeList)
    
    return findDict["".join([str(choose_region), str(choose_zone), str(choose_subzone)])]   
    
def build_array_tree(forest: dict):
    
    index = []  # 一位数组表示二叉树（森林转化后的）森转2后必有左节点，即subzone。subzone存索引
    uid_array = [] #  二维数组用于通过subzone索引找到对应的节点的index
    uid_array = [] # 存储节点uid，通过index获取uid
    
    return index, uid_array

if __name__=="__main__":
    forest = {}  # key region, value Endpoint
    
    ep1 = Endpoint(Region.Asia, Zone.Zone1, SubZone.subZone1, name="ep1")
    ep2 = Endpoint(Region.Asia, Zone.Zone1, SubZone.subZone1, name="ep2")
    ep3 = Endpoint(Region.Asia, Zone.Zone2, SubZone.subZone1, name="ep3")
    ep4 = Endpoint(Region.Asia, Zone.Zone3, SubZone.subZone1, name="ep4")
    ep5 = Endpoint(Region.Europe, Zone.Zone1, SubZone.subZone1, name="ep5")
    ep6 = Endpoint(Region.Europe, Zone.Zone2, SubZone.subZone1, name="ep6")
    ep7 = Endpoint(Region.Europe, Zone.Zone2, SubZone.subZone1, name="ep7")
    ep8 = Endpoint(Region.NorthAmerica, Zone.Zone1, SubZone.subZone1, name="ep8")
    
    epDict = {}
    epDict[ep1.uid] = ep1
    epDict[ep2.uid] = ep2
    epDict[ep3.uid] = ep3
    epDict[ep4.uid] = ep4
    epDict[ep5.uid] = ep5
    epDict[ep6.uid] = ep6
    epDict[ep7.uid] = ep7
    epDict[ep8.uid] = ep8
    
    build_forest(forest, ep1)
    build_forest(forest, ep2)
    build_forest(forest, ep3)
    build_forest(forest, ep4)
    build_forest(forest, ep5)
    build_forest(forest, ep6)
    build_forest(forest, ep7)
    build_forest(forest, ep8)
    
    testWl = Endpoint(Region.Europe, Zone.Zone2, SubZone.subZone1)
    
    # print(testWl)
    # print(forest)
    uid = load_balance_by_forest(testWl=testWl, forest=forest)

    print(epDict[uid].name, epDict[uid].uid, epDict[uid].region, epDict[uid].zone, epDict[uid].subzone)
    
    ###
    findDict = {}  # 编码表 key "001", 001是region/zone/subzone的编码; value [enpoints]
    treeRoot = TreeNode()
    build_bin_tree(treeRoot, ep1, findDict)
    build_bin_tree(treeRoot, ep2, findDict)
    build_bin_tree(treeRoot, ep3, findDict)
    build_bin_tree(treeRoot, ep4, findDict)
    build_bin_tree(treeRoot, ep5, findDict)
    build_bin_tree(treeRoot, ep6, findDict)
    build_bin_tree(treeRoot, ep7, findDict)
    build_bin_tree(treeRoot, ep8, findDict)
    
    # print(findDict)
    print("先序遍历")
    print_tree(treeRoot)
    
    # print("删除节点")
    print()
    delete_bin_tree(treeRoot, ep7, findDict)
    delete_bin_tree(treeRoot, ep6, findDict)
    print(findDict)
    # print()
    print("先序遍历")
    print_tree(treeRoot)
    
    treearray = dump_bintree_array(treeRoot)
    print(treearray)
    print("".join([str(i.value) if i else "-" for i in treearray]))
    
    # 0010102--0-010-----0-0
    result = loadbalcance(ep6, [i.value if i else "-" for i in treearray], findDict)
    for node in result:
        print(node.name)